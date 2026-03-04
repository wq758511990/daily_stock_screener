import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from loguru import logger

class EmailService:
    def __init__(self):
        self.receivers = ["758511990@qq.com"]
        self.smtp_server = "smtp.qq.com"
        self.smtp_port = 587
        self.sender_email = "758511990@qq.com"
        self.sender_password = os.getenv("EMAIL_PASS", "aoatnrafwdhmbdib") # 优先从环境变量读取，若无则使用默认值

    def _markdown_to_html(self, md_content: str) -> str:
        """极简的 Markdown 转换 HTML 工具，支持标题和表格"""
        html = """
        <html>
        <head>
            <style>
                body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; }
                h1 { color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }
                h2 { color: #2980b9; margin-top: 25px; border-left: 4px solid #2980b9; padding-left: 10px; }
                h3 { color: #16a085; }
                table { border-collapse: collapse; width: 100%; margin: 15px 0; background-color: #fff; }
                th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
                th { background-color: #f8f9fa; font-weight: bold; color: #2c3e50; }
                tr:nth-child(even) { background-color: #f2f2f2; }
                tr:hover { background-color: #e9ecef; }
                blockquote { background: #f9f9f9; border-left: 10px solid #ccc; margin: 1.5em 10px; padding: 0.5em 10px; quotes: "\201C""\201D""\2018""\2019"; }
                .highlight { color: #e74c3c; font-weight: bold; }
                code { background-color: #f1f1f1; padding: 2px 4px; border-radius: 4px; font-family: monospace; }
            </style>
        </head>
        <body>
        """
        
        lines = md_content.split('\n')
        in_table = False
        
        for line in lines:
            line = line.strip()
            if not line:
                if in_table:
                    html += "</table>\n"
                    in_table = False
                html += "<br>\n"
                continue

            # 处理标题
            if line.startswith('# '):
                html += f"<h1>{line[2:]}</h1>\n"
            elif line.startswith('## '):
                html += f"<h2>{line[3:]}</h2>\n"
            elif line.startswith('### '):
                html += f"<h3>{line[4:]}</h3>\n"
            # 处理加粗 (简单替换)
            elif '**' in line:
                line = line.replace('**', '<span class="highlight">', 1).replace('**', '</span>', 1)
                html += f"<p>{line}</p>\n"
            # 处理表格
            elif line.startswith('|'):
                if '---' in line: # 跳过分隔行
                    continue
                cells = [c.strip() for c in line.split('|') if c.strip()]
                if not in_table:
                    html += "<table>\n<thead>\n<tr>\n"
                    for cell in cells:
                        html += f"<th>{cell}</th>\n"
                    html += "</tr>\n</thead>\n<tbody>\n"
                    in_table = True
                else:
                    html += "<tr>\n"
                    for cell in cells:
                        # 处理单元格内的代码块
                        if '`' in cell:
                            cell = cell.replace('`', '<code>', 1).replace('`', '</code>', 1)
                        html += f"<td>{cell}</td>\n"
                    html += "</tr>\n"
            # 处理引用
            elif line.startswith('>'):
                html += f"<blockquote>{line[1:].strip()}</blockquote>\n"
            # 处理普通段落
            else:
                if in_table:
                    html += "</table>\n"
                    in_table = False
                html += f"<p>{line}</p>\n"
                
        if in_table:
            html += "</table>\n"
            
        html += "</body></html>"
        return html

    def send_report(self, subject: str, content: str):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = ", ".join(self.receivers)
            msg['Subject'] = subject

            # 转换为 HTML
            html_content = self._markdown_to_html(content)
            
            # 同时提供纯文本和 HTML 版本
            msg.attach(MIMEText(content, 'plain', 'utf-8'))
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            # 创建 SMTP 会话
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            
            # 登录
            server.login(self.sender_email, self.sender_password)
            
            # 发送邮件
            text = msg.as_string()
            server.sendmail(self.sender_email, self.receivers, text)
            
            # 关闭 SMTP 会话
            server.quit()
            logger.info("✅ 报告邮件发送成功！")
        except Exception as e:
            logger.error(f"❌ 报告邮件发送失败: {e}")
