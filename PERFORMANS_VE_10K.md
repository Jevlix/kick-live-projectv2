# Performans ve 10.000 eşzamanlı kullanıcı notları

Bu sürümde öncelik veri alma süresini azaltmak ve aynı verinin tekrar tekrar veritabanında hesaplanmasını engellemektir.

## Bu sürüm neyi değiştiriyor?

### 1. Ağır dashboard verisi cache'leniyor
- Canlı `/api/data` özeti varsayılan olarak 30 saniye cache'lenir.
- Bitmiş yayınların özeti 10 dakika cache'lenir.
- Bitmiş yayınların özeti ayrıca SQLite içindeki `stream_summary_cache` tablosuna kaydedilir; servis yeniden başlasa da yeniden hesaplama gerektirmez.
- Aynı cache anahtarı için 1000 istek aynı anda gelse bile tek bir hesaplama çalıştırılır; kalan istekler aynı sonucu bekler.

### 2. Önceki yayınlar N+1 sorgudan çıkarıldı
Eski yapı kullanıcı başına ve emoji başına ek SQL sorguları çalıştırıyordu. Yeni summary builder toplu `GROUP BY`, `JOIN` ve window query kullanıyor. Bu, özellikle büyük yayınlarda ilk yükleme süresini ciddi biçimde azaltır.

### 3. Canlı dashboard 5 saniyede bir ağır veri çekmiyor
- İlk girişte full snapshot alınır.
- Canlı chat WebSocket üzerinden gelir.
- Sayaçlar `/api/live/stats` üzerinden yaklaşık 30 saniyede bir güncellenir.
- Geçmiş yayınlarda otomatik yenileme yoktur.
- Manuel `Yenile` butonu ağır snapshot'ı zorla yeniden hesaplatabilir.

### 4. WebSocket bootstrap DB'yi gereksiz yere dövmüyor
Yeni kullanıcı önce RAM'deki son chat mesajlarını alır. Sadece servis yeni açılmışsa ve RAM boşsa SQLite'dan son 50 mesaj alınır.

### 5. WebSocket broadcast batching
Chat/event mesajları yaklaşık 200 ms'lik batch'lerde gönderilir. Böylece yüksek chat hızında her event için ayrı ağ döngüsü oluşmaz.

### 6. JSONL loglama batch oldu
Bir DB batch'indeki event'ler aynı log dosyasına tek `open/write/close` işlemiyle yazılır. Chat başına dosya açıp kapatma kaldırıldı.

### 7. Rate limiting
API üzerinde IP bazlı son savunma katmanı bulunuyor:
- `/api/data`: 30 istek/dakika/IP
- detay endpoint'leri: 90 istek/dakika/IP
- diğer API endpoint'leri: 180 istek/dakika/IP

Gerçek üretim ortamında Cloudflare/WAF veya Caddy/Nginx katmanında da rate limit kullanılmalıdır.

### 8. HTTP cache ve sıkıştırma
API cevapları için `Cache-Control` header'ları ve GZip sıkıştırması etkin.

### 9. Saat standardı
Tüm gösterimlerde `Europe/Nicosia` timezone kullanılır. Sunucunun UTC çalışması sorun değildir; frontend UTC timestamp'i Kıbrıs saatine çevirir.

## 10.000 kişi için önemli uyarı

Bu ayarlar sistemi ciddi şekilde daha dayanıklı hale getirir ancak "10.000 eşzamanlı kullanıcı kesinlikle sorunsuz" garantisi anlamına gelmez. Bunu gerçek trafik profiline yakın yük testi ile doğrulamak gerekir.

Özellikle 10.000 kişinin aynı anda **canlı chat WebSocket** kullanması, HTTP sayfalarından daha pahalıdır. Bu yüzden gerçek 10K hedefinde şu mimari önerilir:

```text
Cloudflare / WAF / CDN
        |
   Caddy / Nginx
        |
   FastAPI API
        |
   Redis (shared cache + pub/sub)
        |
   PostgreSQL
        |
   Ayrı Kick recorder
```

Mevcut proje tek process içinde recorder + API çalıştırdığı için doğrudan `--workers 4` açılması önerilmez; her worker ayrı recorder başlatıp Kick'e mükerrer bağlantı açabilir. Çoklu worker aşamasında recorder ayrı servis/process yapılmalıdır.

## Test önerisi

Önce normal test:

```powershell
.\venv\Scripts\python.exe -u -m uvicorn server:app --host 127.0.0.1 --port 8000
```

Sonra:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health | Select-Object -ExpandProperty Content
```

Üretime almadan önce k6 veya benzeri bir araçla özellikle şu endpoint'ler ölçülmelidir:

```text
GET /api/data?mode=stream&stream_id=...
GET /api/streams
GET /api/live/stats
WS /ws
```

Amaç sadece "site açılıyor" değil; p95/p99 latency, hata oranı, CPU, RAM, SQLite lock süresi ve WebSocket bağlantı sayısını görmektir.
