class Event:
    def __init__(self, channel: str, message: str) -> None:
        self.channel = channel
        self.message = message

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Event) and self.channel == other.channel and self.message == other.message

    def __repr__(self) -> str:
        return f"Event(channel={self.channel!r}, message={self.message!r})"
