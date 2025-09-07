import pandas as pd

class Services:
    """ Service layer for managing document formatting and mapping """
    
    @staticmethod
    def format_document(documents:list[dict]):
        df = pd.DataFrame(documents)
        df.Antisemitic   = df.Antisemitic.astype(int).astype(bool)
        df.CreateDate  = pd.to_datetime(df.CreateDate, errors="coerce", utc=True)
        df.TweetID = df.TweetID.astype(str)
        df.dropna(inplace=True)
        return df.to_dict(orient='records')
    
        
    query_to_delete = {
        "bool": 
            { 
                "must": 
                    [
                        {"term": {"Antisemitic": False}},
                        {"terms": {"sentiment": ["Neutral", "Positive"]}}
                    ],
                "must_not": 
                    [
                        {"exists": {"field": "weapons_detected"}}
                    ]
            }
        }

    
    query_to_get_antisemitic_documents = {
        "bool": 
            { 
                "must": 
                    [
                        {"term": {"Antisemitic": True}},
                        {"exists": {"field": "weapons_detected"}}
                    ]
            }
        }
    query_to_get_weapons_documents = {
        "bool": 
            { 
                "must": 
                    [
                        {"exists": {"field": "weapons_detected"}},
                        {"script": {"script": {
                                "source": "doc['weapons_detected'].size() > 2",
                                "lang": "painless"}
                            }
                        }
                    ]
            }
        }


    
