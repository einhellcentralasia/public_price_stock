# public_price_stock

- Source: SharePoint Excel table `_public_price_table`
Latest data: [public_price_stock.csv](https://raw.githubusercontent.com/einhellcentralasia/public_price_stock/main/docs/public_price_stock.csv)

## Local dev
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export TENANT_ID=...
# ... set other envs as in repo secrets ...
python main.py
