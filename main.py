import json, os
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request,  Header, BackgroundTasks, HTTPException, status
from google import genai
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageSendMessage, AudioMessage

# 設定 Google AI API 金鑰
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# 設定生成文字的參數 + 角色扮演
system_instruction = "你是一位AI教父黃仁勳,每次回答問題之餘,還會講一個150字以內的笑話! "
thinking_config = genai.types.ThinkingConfig(thinking_budget=0) # thinking_budget = 0,  turn off thinking mode
generation_config = genai.types.GenerateContentConfig(max_output_tokens=512, temperature=0.5, top_p=0.5,
                                                      thinking_config=thinking_config,
                                                      system_instruction=system_instruction)

# 設定 Line Bot 的 API 金鑰和秘密金鑰
line_bot_api = LineBotApi(os.environ["CHANNEL_ACCESS_TOKEN"])
line_handler = WebhookHandler(os.environ["CHANNEL_SECRET"])

# 設定是否正在與使用者交談
working_status = os.getenv("DEFALUT_TALKING", default = "true").lower() == "true"

# 建立 FastAPI 應用程式
app = FastAPI()

# 設定 CORS，允許跨域請求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 處理根路徑請求
@app.get("/")
def root():
    return {"title": "Line Bot"}

# 處理 Line Webhook 請求
@app.post("/webhook")
async def webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_line_signature=Header(None),
):
    # 取得請求內容
    body = await request.body()
    try:
        # 將處理 Line 事件的任務加入背景工作
        background_tasks.add_task(
            line_handler.handle, body.decode("utf-8"), x_line_signature
        )
    except InvalidSignatureError:
        # 處理無效的簽章錯誤
        raise HTTPException(status_code=400, detail="Invalid signature")
    return "ok"
    
# 處理文字訊息事件
chat_sessions = {}
@line_handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    global working_status
    
    # 檢查事件類型和訊息類型
    if event.type != "message" or event.message.type != "text":
        # 回覆錯誤訊息
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="Event type error:[No message or the message does not contain text]")
        )
        
    # 檢查使用者是否輸入 "再見"
    elif event.message.text == "再見":
        # 回覆 "Bye!"
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="Bye!")
        )
        return
       
    # 檢查是否正在與使用者交談
    elif working_status:
        try: 
            user_id = event.source.user_id
            chat = chat_sessions.get(user_id) or client.chats.create(model="gemini-3-flash-preview", config=generation_config)
            chat_sessions[user_id] = chat
            # 取得使用者輸入的文字
            user_input = event.message.text
            # 發送使用者的輸入到聊天會話並獲得回應
            response = chat.send_message(user_input)
            if (response.text != None):
                # 取得生成結果
                out = response.text
            else:
                out = "Gemini沒答案!請換個說法！"
        except:
            # 處理錯誤
            out = "Gemini執行出錯!請換個說法！" 
  
        # 回覆生成結果
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=out))

if __name__ == "__main__":
    # 啟動 FastAPI 應用程式
    uvicorn.run("main:app", host="0.0.0.0", port=7860, reload=True)
