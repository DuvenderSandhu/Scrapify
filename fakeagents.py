from fake_useragent import UserAgent
import random
ua_platform= ['desktop','mobile','tablet']
ua_os= ["Windows", "Linux", "Ubuntu", "Chrome OS", "Mac OS X", "Android", "iOS"]

def get_random_user_agent():
    ua = UserAgent(os=random.choice(ua_os), platforms=random.choice(ua_platform))