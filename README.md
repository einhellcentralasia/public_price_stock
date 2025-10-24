# public_price_stock

- Source: SharePoint Excel table `_public_price_table`
- Output: `docs/public_price_stock.xml` (all columns; `Stock` bucketed; `updatedAt` added `dd.mm.yyyy hh:mm`)
- Deployed to Cloudflare Pages: `public-price-stock.pages.dev` (project `public_price_stock`)
- Schedule: every ~12h with 72h gate (saves CI minutes)

## Local dev
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export TENANT_ID=...
# ... set other envs as in repo secrets ...
python main.py
