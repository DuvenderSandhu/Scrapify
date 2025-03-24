import smtplib
from email.mime.text import MIMEText

def send_email(messageHTML: str="<p>Hello,</p><p>Your scraping has been completed.</p><p>You can download your data from the link below:</p><p><a href='http://100.42.181.89:8502/?tab=tab2'>Visit Site</a> and Go to <b>Result Tab</b> and Choose Your Website</p><p>Thank you.</p>") -> None:
    """
    Function to send an email using Gmail's SMTP server. 
    All parameters are hardcoded within the function.
    """
    # Hardcoded values for email sending
    emails= ""
    with open('email.txt', 'r') as file:
        # Read the content of the file
        content = file.read()
        emails= content
    sender_email = "androidwriters1@gmail.com"#"factorypricescrapper@gmail.com"
    receiver_email = emails.split(",")
    password = "cjuz tjzx zotj uvjz"#"pfqq rmsr vngz pkys"  # Or app-specific password for Gmail
    subject = "Scraping Status – Web Scrapper"
    html_body = messageHTML

    # Create the email content
    message = MIMEText(html_body, "html")
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message["Subject"] = subject

    try:
        # Set up the SMTP server and send the email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()  # Secure the connection
            server.login(sender_email, password)  # Log in with email and password
            server.sendmail(sender_email, receiver_email, message.as_string())  # Send the email
        
        print("Email sent successfully!")

    except Exception as e:
        print(f"Failed to send email: {e}")

# send_email()