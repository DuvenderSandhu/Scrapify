import smtplib
from email.mime.text import MIMEText

def send_email():
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
    sender_email = "factorypricescrapper@gmail.com"
    receiver_email = emails.split(",")
    password = "pfqq rmsr vngz pkys"  # Or app-specific password for Gmail
    subject = "Scraping Completed – Download Your Data"
    html_body = "<p>Hello,</p><p>Your scraping from <strong>Coldwell Banker Homes</strong> has been completed.</p><p>You can download your data from the link below:</p><p><a href='https://e07f-2405-201-7003-5074-8ef5-8932-c6db-350.ngrok-free.app/?tab=tab2'>Visit Site</a> and Go to <b>Result Tab</b></p><p>Thank you.</p>"

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