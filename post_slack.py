import requests
import json

def post_slack():
    webhook_url = "https://hooks.slack.com/services/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"
    emoji_text = ':sunglasses::hushed:'
    req_text = 'Machine learning methods in quantum computing theory https://t.co/D0JrcYkzqL あとで読む'

    requests.post(webhook_url, data=json.dumps({
        "text": emoji_text,
        'username': 'yone\'s-test-bot',
        'icon_emoji': ':sunglasses:',
        "attachments": [
            {
                "text": req_text,
                "color": "#3AA3E3",
            }
        ]
    }))


if __name__ == "__main__":
    post_slack()
