import json
import re
import logging
from core.ai_provider import ai_provider

log = logging.getLogger("PegasusExtract")

def clean_code(text: str) -> str:
    """Strip markdown fences and extra text from AI response."""
    # Remove ```python ... ``` blocks
    text = re.sub(r'```python\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    # Remove any lines before 'import' or 'class'
    lines = text.split('\n')
    start = 0
    for i, line in enumerate(lines):
        if line.strip().startswith(('import ', 'from ', 'class ')):
            start = i
            break
    return '\n'.join(lines[start:]).strip()

async def generate_adapter(plan: dict) -> str:
    SYSTEM = """You are an expert Python web scraping engineer.
Return ONLY valid Python code. No explanations, no markdown, 
no backticks. Start directly with import statements."""

    USER = f"""Write a Python class called DynamicSiteAdapter.

Plan:
{json.dumps(plan, indent=2)}

Requirements:
1. __init__(self, plan): store plan
2. get_input_records(self, config=None) -> list:
   Return list of dicts with 'url' key.
   For paginated: generate URLs like 
   https://books.toscrape.com/catalogue/page-1.html
   up to plan['crawler_config']['max_pages']
3. async extract_page(self, page, record, config=None) -> list:
   Use selectors from plan to extract data.
   Return list of dicts.
4. csv_columns(self) -> list: return field names
5. to_csv_rows(self, record) -> list: return [record]

Use only: re, json, logging
Handle all errors with try/except.
Start code with: import re"""

    result = await ai_provider.complete(SYSTEM, USER)
    code = clean_code(result["text"])
    log.info(f"Generated adapter code ({len(code)} chars)")
    
    # Validate syntax before returning
    try:
        compile(code, "<adapter>", "exec")
        log.info("Adapter syntax OK")
    except SyntaxError as e:
        log.error(f"Syntax error in generated code: {e}")
        # Return a safe fallback adapter
        code = _fallback_adapter(plan)
    
    return code

def _fallback_adapter(plan: dict) -> str:
    """Hardcoded fallback for books.toscrape.com style sites."""
    max_pages = plan.get('crawler_config', {}).get('max_pages', 3)
    base_url = plan.get('target_url', 'https://books.toscrape.com')
    container = plan.get('extraction_config', {}).get('container_selector', 'article.product_pod')
    fields = plan.get('extraction_config', {}).get('fields', {})
    
    fields_code = ""
    columns = []
    for field_name, field_config in fields.items():
        selector = field_config.get('selector', '')
        attribute = field_config.get('attribute', 'text')
        columns.append(field_name)
        if attribute == 'text':
            fields_code += f"""
        try:
            el = await item.query_selector("{selector}")
            record["{field_name}"] = (await el.inner_text()).strip() if el else ""
        except:
            record["{field_name}"] = ""
"""
        else:
            fields_code += f"""
        try:
            el = await item.query_selector("{selector}")
            record["{field_name}"] = await el.get_attribute("{attribute}") if el else ""
        except:
            record["{field_name}"] = ""
"""

    return f'''import re
import logging

log = logging.getLogger("PegasusExtract")

class DynamicSiteAdapter:
    def __init__(self, plan):
        self.plan = plan
        self.max_pages = {max_pages}
        self.base_url = "{base_url}"

    def get_input_records(self, config=None):
        records = []
        for i in range(1, self.max_pages + 1):
            if i == 1:
                records.append({{"url": self.base_url}})
            else:
                records.append({{"url": f"{{self.base_url}}catalogue/page-{{i}}.html"}})
        return records

    async def extract_page(self, page, record, config=None):
        results = []
        try:
            await page.goto(record["url"], timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(1000)
            items = await page.query_selector_all("{container}")
            log.info(f"Found {{len(items)}} items on {{record['url']}}")
            for item in items:
                record = {{}}
{fields_code}
                if any(record.values()):
                    results.append(record)
        except Exception as e:
            log.error(f"Page extraction failed: {{e}}")
        return results

    def csv_columns(self):
        return {columns}

    def to_csv_rows(self, record):
        return [record]
'''
