from WebChat import WebChat
from ChatLLM import LocalChatGLM3


llm = LocalChatGLM3('/home/sleepy/Depot/Models/chatglm3-6b')
web_chat = WebChat(llm)

llm.async_init_llm()
web_chat.setup_web_chat()
