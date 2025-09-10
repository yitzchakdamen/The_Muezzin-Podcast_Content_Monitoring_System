
from collections import Counter
import math
import base64, string


class TextAnalysis:
    """ 
    Text analysis for rating - hostile
    Contains a decoding function, 
    an analysis function, 
    and a full process execution function
    """
    
    def __init__(
                self, 
                hostile_text:str,
                less_hostile_text:str,
                high_danger_level:float,
                medium_danger_level:float,
                threshold:float
                ) -> None:

        self.hostile_text:list[str] = self.text_decoding(hostile_text).lower().split(',')
        self.less_hostile_text:list[str] = self.text_decoding(less_hostile_text).lower().split(',')
        self.high_danger_level = high_danger_level
        self.medium_danger_level = medium_danger_level
        self.threshold = threshold

    def analysis_process(self, text: str) -> dict:
        """ Full text analysis process """
        text_list:list = self.text_cleaning(text)
        classification = self.content_classification(text_list)
        return {
            "bds_percent": self.percentage_danger(score=classification, text=text_list),
            "is_bds" : self.criminal_event_threshold(classification),
            "bds_threat_level": self.segmentation_of_danger_levels(classification)
        }
    
    def text_decoding(self, text: str) -> str:
        """Decrypting base64 encoded text"""
        return base64.b64decode(text.encode('utf-8')).decode('utf-8')
    
    def text_cleaning(self, text:str) -> list:
        """ Cleaning the text - removing punctuation and splitting into words"""
        translator = str.maketrans('', '', string.punctuation)
        return text.lower().translate(translator).split()
    
    def content_classification(self, text:list) -> float:
        """
        Classifying the text - hostile or not hostile
        Using a weighted scoring system
        The more hostile words, the higher the score
        The score is logarithmic to avoid very high scores for very long texts
        0.01 is added to the score to avoid division by zero
        100 is the maximum score
        """
        total_words = len(text)
        word_counts = Counter(text)
        weight_w = 100 /  math.log(1 + total_words)

        word_weights = {word: weight_w * 2 for word in self.hostile_text}
        word_weights.update({word: weight_w * 1 for word in self.less_hostile_text})

        score = 0
        lest_word = ""
        for word, count in word_counts.items():
            weight = word_weights.get(word, 0)
            if weight <= 0 : weight = word_weights.get(f"{lest_word} {word}", 0)
            if weight > 0: score += weight * math.log(1 + count)
            lest_word = word
    
        max_score = sum([ weight_w * math.log(1 + count) for count in word_counts.values()])
        return min(max_score / (score or 0.01), 100)

    
    def percentage_danger(self, score: float, text:list) -> float:
        """ Percentage of hostile content in the text"""
        return  score # score * 100  / len(text)
    
    def criminal_event_threshold(self, score: float) -> bool:
        """ Determining if the text is hostile based on a threshold"""
        return  score > self.threshold
    
    def segmentation_of_danger_levels(self, score: float) -> str:
        """ Segmenting the danger levels into high, medium, and none"""
        if score > self.high_danger_level : return  "high" 
        elif score > self.medium_danger_level : return "medium" 
        return "none"
