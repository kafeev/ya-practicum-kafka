import faust
import logging


from models import Message, BlockEvent, BannedWord

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


app = faust.App("message-filter-app", broker="kafka://kafka1:9092")

messages_topic = app.topic("messages", value_type=Message, partitions=1)

filtered_topic = app.topic("filtered_messages", value_type=Message, partitions=1)

blocked_topic = app.topic("blocked_users", value_type=BlockEvent, partitions=1)

banned_words_topic = app.topic("banned_words", value_type=BannedWord, partitions=1)

blocked_users_table = app.Table("blocked_users_table", default=set, partitions=1)

banned_words_table = app.Table("banned_words_table", default=set, partitions=1)


@app.agent(blocked_topic)
async def update_blocked_users(stream):

    async for event in stream:

        blocked = blocked_users_table[event.user]

        blocked.add(event.blocked_user)

        blocked_users_table[event.user] = blocked

        logger.info(f"[BLOCK] {event.user} blocked {event.blocked_user}")


@app.agent(banned_words_topic)
async def update_banned_words(stream):

    async for event in stream:

        words = banned_words_table["words"]

        words.add(event.word)

        banned_words_table["words"] = words

        logger.info(f"[BANNED WORD] {event.word}")

# заменяем запрещенные слова на "****"
def censor_text(text: str) -> str:

    words = banned_words_table["words"]

    for word in words:
        text = text.replace(word, "*" * len(word))

    return text


@app.agent(messages_topic)
async def process_messages(stream):

    async for message in stream:

        blocked = blocked_users_table[message.receiver]

        if message.sender in blocked:

            logger.info(f"[MESSAGE BLOCKED] " f"{message.sender} -> {message.receiver}")

            continue

        message.text = censor_text(message.text)

        await filtered_topic.send(value=message)

        logger.info(
            f"[MESSAGE DELIVERED] "
            f"{message.sender} -> "
            f"{message.receiver}: "
            f"{message.text}"
        )


if __name__ == "__main__":
    app.main()
