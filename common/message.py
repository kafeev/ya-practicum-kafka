import json
from datetime import datetime

class Message:
  """Класс сообщения с сериализацией в JSON."""
  def __init__(self, id: int, text: str, timestamp: str = None):
    self.id = id
    self.text = text
    self.timestamp = timestamp or datetime.utcnow().isoformat()
  
  def to_json(self) -> str:
    """Сериализация в строку JSON."""
    return json.dumps({
      'id': self.id,
      'text': self.text,
      'timestamp': self.timestamp
    })
  
  @staticmethod
  def from_json(json_str: str):
    """Десериализация из JSON."""
    try:
      data = json.loads(json_str)
      return Message(data['id'], data['text'], data['timestamp'])
    except Exception as e:
      raise ValueError(f"Deserialization error: {e}")
  
  def __str__(self):
    return f"Message(id={self.id}, text='{self.text}', timestamp={self.timestamp})"