# Footshop Scraper

Automated scraper for Footshop fashion store with embeddings and Supabase integration.

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure Supabase (optional)

The scraper uses the credentials in `config.py` by default. To use environment variables instead:

```bash
export SUPABASE_URL="your_supabase_url"
export SUPABASE_KEY="your_supabase_anon_key"
```

### 3. Run locally

```bash
python main.py
```

## GitHub Actions Setup

### 1. Add GitHub Secrets

Go to your repository settings → Secrets and variables → Actions, and add:

- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_KEY`: Your Supabase anon key

### 2. Automated Runs

The scraper runs automatically:

- **Daily at midnight** (UTC) - scheduled via `cron: '0 0 * * *'`
- **Manually** - use the "Run workflow" button in GitHub Actions
- **On push** to main branch

### 3. Run Manually

1. Go to Actions tab
2. Select "Footshop Scraper"
3. Click "Run workflow"
4. Select branch and click "Run workflow"

## Output Files

- `product_urls.json` - All collected product URLs
- `scraped_products.json` - Products with details (before embeddings)
- `products_with_embeddings.json` - Complete products with embeddings

## Database Schema

The scraper inserts into the `products` table with:
- `source` = "scraper-footshop"
- `brand` = "Footshop"  
- `image_embedding` - 768-dim SigLIP embedding from product image
- `info_embedding` - 768-dim SigLIP embedding from product info text
- All product details (title, price, category, gender, etc.)
