import sqlite3
import pandas as pd
import os

# Konfigurasi Path
# Pastikan path ini sesuai dengan lokasi file db di komputer Anda
db_file = 'data/news_articles.db' 
csv_file = 'data/hasil_berita.csv'

# Cek apakah database ada
if not os.path.exists(db_file):
    print(f"Error: File database tidak ditemukan di {db_file}")
else:
    # 1. Koneksi ke Database
    conn = sqlite3.connect(db_file)
    
    try:
        # 2. Jalankan Query SQL
        query = "SELECT * FROM news_articles"
        df = pd.read_sql_query(query, conn)
        
        # 3. Simpan ke CSV
        df.to_csv(csv_file, index=False)
        
        print(f"✅ Berhasil! Data telah diexport ke: {csv_file}")
        print(f"📊 Total Baris: {len(df)}")
        print("\nContoh 5 data teratas:")
        print(df.head())
        
    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")
        
    finally:
        conn.close()