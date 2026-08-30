package claude

import "fmt"

// systemPrompt is byte-for-byte identical across every request in every batch,
// which is what makes it worth a cache breakpoint: the batch pays to write it
// once and reads it back for the remaining requests.
//
// Two rules carry most of the weight. "Never guess a date" is what keeps
// low-confidence junk out of the calendar, and "JSON only" is what keeps the
// parser from having to understand prose.
const systemPrompt = `คุณคือระบบสกัดข้อมูล event จากโพสต์ Instagram ภาษาไทย
ตอบเป็น JSON เท่านั้นตาม schema:
{
  "is_event": boolean,
  "confidence": "high" | "medium" | "low",
  "title": string | null,
  "venue_name": string | null,
  "start_date": string | null,
  "end_date": string | null,
  "price_text": string | null,
  "category": string | null,
  "registration_url": string | null
}

กติกา:
- ถ้าไม่ใช่โพสต์เกี่ยวกับ event/workshop/exhibition ให้ is_event=false, field อื่น null
- ถ้าเดาวันที่ไม่ได้ชัดเจน ให้ start_date=null และ confidence="low" — ห้ามเดามั่ว
- ใช้ posted_at ที่ให้มาช่วย resolve คำเชิงสัมพัทธ์ เช่น "เสาร์นี้", "พรุ่งนี้"
- start_date และ end_date ต้องเป็นรูปแบบ ISO 8601 date (YYYY-MM-DD) เท่านั้น
- venue_name ให้ใส่ชื่อสถานที่ตามที่เขียนในโพสต์ ไม่ต้องแปลหรือเดาชื่อเต็ม
- ตอบเป็น JSON object ดิบอย่างเดียว ห้ามใส่ markdown code fence หรือคำอธิบายใดๆ`

// userMessage carries the two facts the model needs per post. posted_at comes
// first because it is the anchor every relative date in the caption resolves
// against.
func userMessage(postedAt string, caption string) string {
	return fmt.Sprintf("posted_at: %s\ncaption: %s", postedAt, caption)
}
