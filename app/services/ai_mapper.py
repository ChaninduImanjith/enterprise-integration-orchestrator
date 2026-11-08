import json
import logging
import re
from typing import Any, Dict, Optional
import httpx
from app.core.config import settings
from app.core.metrics import AI_SCHEMA_MAPPINGS_TOTAL
from app.models.schemas import OrderItem, OrderPayload

logger = logging.getLogger(__name__)

class AISchemaMapper:
    """
    AI-powered dynamic schema mapping engine using Meta Llama 3 API.
    Converts unstructured, heterogeneous, or legacy enterprise payloads
    into canonical, strictly-typed Pydantic OrderPayload instances.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None
    ):
        self.api_key = api_key or settings.LLAMA_API_KEY
        self.base_url = (base_url or settings.LLAMA_API_BASE_URL).rstrip("/")
        self.model = model or settings.LLAMA_MODEL
        self.target_schema = OrderPayload.model_json_schema()

    def _clean_llm_json_response(self, content: str) -> Dict[str, Any]:
        """
        Strips markdown code blocks, whitespace, or commentary to extract pure JSON.
        """
        cleaned = content.strip()
        
        # Remove markdown code block if present: ```json ... ``` or ``` ... ```
        code_block_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", cleaned)
        if code_block_match:
            cleaned = code_block_match.group(1).strip()
            
        return json.loads(cleaned)

    def _fallback_heuristic_mapper(self, legacy_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Intelligent deterministic heuristic fallback when LLM API key is not configured.
        Maps common legacy enterprise field variations into the target schema.
        """
        logger.warning("LLAMA_API_KEY not configured. Executing fallback heuristic schema mapper.")
        
        # 1. Customer ID & Email
        customer_id = (
            legacy_data.get("customer_id")
            or legacy_data.get("customerId")
            or legacy_data.get("client_id")
            or legacy_data.get("account_number")
            or legacy_data.get("user", {}).get("id")
            or "CUST-ANONYMOUS"
        )
        
        customer_email = (
            legacy_data.get("customer_email")
            or legacy_data.get("email")
            or legacy_data.get("contact_email")
            or legacy_data.get("user", {}).get("email")
            or f"{str(customer_id).lower()}@enterprise-domain.internal"
        )
        
        # 2. Currency
        currency = (
            legacy_data.get("currency")
            or legacy_data.get("curr")
            or legacy_data.get("currency_code")
            or "USD"
        )
        
        # 3. Items extraction
        raw_items = (
            legacy_data.get("items")
            or legacy_data.get("line_items")
            or legacy_data.get("products")
            or legacy_data.get("order_lines")
            or []
        )
        
        mapped_items = []
        calculated_total = 0.0
        
        for idx, item in enumerate(raw_items, start=1):
            if not isinstance(item, dict):
                continue
            item_id = str(
                item.get("item_id")
                or item.get("sku")
                or item.get("product_id")
                or item.get("code")
                or f"ITEM-{idx}"
            )
            name = str(
                item.get("name")
                or item.get("title")
                or item.get("description")
                or f"Product {item_id}"
            )
            qty = int(item.get("quantity") or item.get("qty") or item.get("count") or 1)
            price = float(item.get("unit_price") or item.get("price") or item.get("cost") or 0.0)
            
            mapped_items.append({
                "item_id": item_id,
                "name": name,
                "quantity": max(1, qty),
                "unit_price": max(0.0, price)
            })
            calculated_total += qty * price

        if not mapped_items:
            # Fallback single item
            mapped_items.append({
                "item_id": "ITEM-DEFAULT",
                "name": "General Order Item",
                "quantity": 1,
                "unit_price": float(legacy_data.get("total_amount") or legacy_data.get("amount") or 100.0)
            })
            calculated_total = mapped_items[0]["unit_price"]

        # 4. Total Amount
        total_amount = float(
            legacy_data.get("total_amount")
            or legacy_data.get("amount")
            or legacy_data.get("total")
            or legacy_data.get("order_total")
            or calculated_total
        )

        # 5. Order ID
        order_id = (
            legacy_data.get("order_id")
            or legacy_data.get("orderId")
            or legacy_data.get("reference_number")
            or legacy_data.get("invoice_no")
        )

        result = {
            "customer_id": str(customer_id),
            "customer_email": str(customer_email),
            "currency": str(currency).upper(),
            "total_amount": round(total_amount, 2),
            "items": mapped_items,
            "metadata": {"legacy_source_keys": list(legacy_data.keys()), "mapping_method": "heuristic_fallback"}
        }
        if order_id:
            result["order_id"] = str(order_id)
            
        AI_SCHEMA_MAPPINGS_TOTAL.labels(method="heuristic_fallback", status="success").inc()
        return result

    async def map_legacy_payload_to_order(self, legacy_data: Dict[str, Any]) -> OrderPayload:
        """
        Transforms arbitrary legacy JSON into strict OrderPayload using Meta Llama 3.
        Falls back gracefully to deterministic mapping if API key is omitted or API is unreachable.
        """
        # If API key is not present, use the heuristic mapper
        if not self.api_key:
            mapped_dict = self._fallback_heuristic_mapper(legacy_data)
            return OrderPayload.model_validate(mapped_dict)

        system_prompt = (
            "You are an expert enterprise integration data engineering agent.\n"
            "Your task is to transform arbitrary, unstructured, or legacy JSON input into a strictly valid JSON object "
            "that conforms exactly to the following target JSON schema:\n\n"
            f"{json.dumps(self.target_schema, indent=2)}\n\n"
            "CRITICAL INSTRUCTIONS:\n"
            "1. Output ONLY a valid raw JSON object. Do not wrap in markdown or backticks.\n"
            "2. Ensure all required fields exist: customer_id, customer_email, total_amount, items.\n"
            "3. Each item in 'items' must have 'item_id', 'name', 'quantity' (integer > 0), and 'unit_price' (float >= 0).\n"
            "4. Do not include explanatory text before or after the JSON."
        )

        user_content = (
            "Please map the following legacy enterprise payload into the target schema:\n\n"
            f"{json.dumps(legacy_data, indent=2)}"
        )

        request_payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                logger.info(f"Dispatching payload schema mapping to Meta Llama 3 model '{self.model}' at {url}...")
                response = await client.post(url, json=request_payload, headers=headers)
                response.raise_for_status()
                
                resp_data = response.json()
                raw_content = resp_data["choices"][0]["message"]["content"]
                
                parsed_json = self._clean_llm_json_response(raw_content)
                logger.info("Successfully received and parsed mapped JSON from Meta Llama 3.")
                
                mapped_order = OrderPayload.model_validate(parsed_json)
                AI_SCHEMA_MAPPINGS_TOTAL.labels(method="llama3_api", status="success").inc()
                return mapped_order

        except Exception as exc:
            logger.exception(f"Llama 3 API mapping failed ({exc}). Engaging heuristic fallback...")
            AI_SCHEMA_MAPPINGS_TOTAL.labels(method="llama3_api", status="fallback").inc()
            fallback_dict = self._fallback_heuristic_mapper(legacy_data)
            return OrderPayload.model_validate(fallback_dict)

ai_mapper_service = AISchemaMapper()
