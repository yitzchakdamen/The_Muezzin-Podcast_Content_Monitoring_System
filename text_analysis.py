
import base64, string


class TextAnalysis:
    
    def __init__(self, hostile_text:str, less_hostile_text:str) -> None:

        self.hostile_text:list[str] = self.text_decoding(hostile_text).split(',')
        self.less_hostile_text:list[str] = self.text_decoding(less_hostile_text).split(',')
    
    def snalysis_process(self, text: str) -> dict:
        classification = self.content_classification(text)
        return {
            "bds_percent": self.percentage_danger(classification),
            "is_bds" : self.criminal_event_threshold(classification),
            "bds_threat_level": self.segmentation_of_danger_levels(classification)
        }
    
    def text_decoding(self, text: str) -> str:
        return base64.b64decode(text.encode('utf-8')).decode('utf-8')
    
    def text_cleaning(self, text:str) -> list:
        translator = str.maketrans('', '', string.punctuation)
        return text.lower().translate(translator).split()
    
    def content_classification(self, text) -> int:
        text = self.text_cleaning(text)
        score = 0
        for i, leteer in enumerate(text):
            try:
                if leteer in self.less_hostile_text or f"{leteer} {text[i + 1]}" in self.less_hostile_text: score += 1
                elif leteer in self.hostile_text or f"{leteer} {text[i + 1]}" in self.hostile_text: score += 2
                else: score -= 1
            except:
                pass
        print(score, len(text))
        return score
    
    def percentage_danger(self, score: int) -> float:
        return  score / 100
    
    def criminal_event_threshold(self, score: int) -> bool:
        return  score > 70
    
    def segmentation_of_danger_levels(self, score: int) -> str:
        if score > 70 : return  "medium" 
        elif score > 20 : return "high" 
        return "none"


j = "The new cycle moves fast, but Gaza doesn't disappear when cameras do. The blockade is still there, and so is the humanitarian crisis. Exactly. I read a report yesterday. It said, malnutrition is spreading among children. That's a war crime in itself. Meanwhile, refugees keep growing in number, and displacement means whole communities are erased. The protests worldwide are encouraging, though. From London to New York, people chant for a ceasefire and free Palestine. And linking it back to BDS, it's about applying pressure where governments fail. Right. Liberation isn't easy, but the people's resilience is inspiring. Resistance can be cultural, political, and global. And podcasts like ours? Just small ripples. But ripples matter."

a = "R2Vub2NpZGUsV2FyIENyaW1lcyxBcGFydGhlaWQsTWFzc2FjcmUsTmFrYmEsRGlzcGxhY2VtZW50LEh1bWFuaXRhcmlhbiBDcmlzaXMsQmxvY2thZGUsT2NjdXBhdGlvbixSZWZ1Z2VlcyxJQ0MsQkRT"
b = "RnJlZWRvbSBGbG90aWxsYSxSZXNpc3RhbmNlLExpYmVyYXRpb24sRnJlZSBQYWxlc3RpbmUsR2F6YSxDZWFzZWZpcmUsUHJvdGVzdCxVTlJXQQ=="
print(TextAnalysis(hostile_text=b, less_hostile_text=a).snalysis_process(text=j))