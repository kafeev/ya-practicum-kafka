import faust


class Message(faust.Record):
    sender: str
    receiver: str
    text: str


class BlockEvent(faust.Record):
    user: str
    blocked_user: str


class BannedWord(faust.Record):
    word: str
