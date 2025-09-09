
import base64, string


class TextAnalysis:
    
    def __init__(self, hostile_text:str, less_hostile_text:str) -> None:

        self.hostile_text:list[str] = self.text_decoding(hostile_text).lower().split(',')
        self.less_hostile_text:list[str] = self.text_decoding(less_hostile_text).lower().split(',')
    
    def snalysis_process(self, text: str) -> dict:
        text_list:list = self.text_cleaning(text)
        classification = self.content_classification(text_list)
        return {
            "bds_percent": self.percentage_danger(score=classification, text=text_list),
            "is_bds" : self.criminal_event_threshold(classification),
            "bds_threat_level": self.segmentation_of_danger_levels(classification)
        }
    
    def text_decoding(self, text: str) -> str:
        return base64.b64decode(text.encode('utf-8')).decode('utf-8')
    
    def text_cleaning(self, text:str) -> list:
        translator = str.maketrans('', '', string.punctuation)
        return text.lower().translate(translator).split()
    
    def content_classification(self, text:list) -> float:
        score = 0
        for i, leteer in enumerate(text):
            if leteer in self.less_hostile_text or f"{leteer} {text[i + 1]}" in self.less_hostile_text: score += 1
            elif leteer in self.hostile_text or f"{leteer} {text[i + 1]}" in self.hostile_text: score += 2
        return score 
    
    def percentage_danger(self, score: float, text:list) -> float:
        return  score * 100  / len(text)
    
    def criminal_event_threshold(self, score: float) -> bool:
        return  score > 60
    
    def segmentation_of_danger_levels(self, score: float) -> str:
        if score > 70 : return  "high" 
        elif score > 40 : return "medium" 
        return "none"
