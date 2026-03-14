import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = "https://yqawmzggcgpeyaaynrjk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlxYXdtemdnY2dwZXlhYXlucmprIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1NTAxMDkyNiwiZXhwIjoyMDcwNTg2OTI2fQ.XtLpxausFriraFJeX27ZzsdQsFv3uQKXBBggoz6P4D4"

CATEGORIES = [
    "https://www.ftshp.be/en/5-mens-shoes",
    "https://www.ftshp.be/en/6-womens-shoes"
]

SOURCE = "scraper-footshop"
BRAND = "Footshop"
SECOND_HAND = False

EMBEDDING_MODEL = "google/siglip-base-patch16-384"
EMBEDDING_DIMENSION = 768
