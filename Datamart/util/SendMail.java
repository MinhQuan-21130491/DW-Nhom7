package util;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The MailService class provides methods to send emails using JavaMail API.
 * It includes functionality to send plain text emails and emails with attachments.
 * The email configuration is loaded from a properties file.
 *
 * Usage:
 * - To send a plain text email:
 *   MailService.sendMail(to, subject, content);
 *
 * - To send an email with an attachment:
 *   MailService.sendMail(to, subject, content, filePath);
 *
 * The email configuration (email address and password) is read from the "config.properties" file.
 * The properties file should contain the following keys:
 * - email_service: The email address used for sending emails.
 * - password_mail_service: The password associated with the email address.
 *
 * Example usage:
 * ```java
 * public static void main(String[] args) {
 *     String email = "recipient@example.com";
 *     String content = "This is the content of the email.";
 *     MailService.sendMail(email, "Subject of the Email", content);
 * }
 * ```
 */
public class SendMail {
    private static final String FILE_CONFIG = "\\config.env";
    private static String email;
    private static String password;

    /**
     * Initializes the email and password fields by loading them from the "config.properties" file.
     */
    static{
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            String currentDir = System.getProperty("user.dir");
            inputStream = new FileInputStream(currentDir + FILE_CONFIG);
            // load properties from file
            properties.load(inputStream);
            // get property by name
            email = properties.getProperty("email_service");
            password = properties.getProperty("password_mail_service");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // close objects
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Establishes a connection to the email server and returns a Session object.
     *
     * @return A Session object for sending emails.
     */
    public static Session connect(){
        // Assuming you are sending email from through gmails smtp
        String host = "smtp.gmail.com";

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.put("mail.smtp.host", host);
        //465, 587
        properties.put("mail.smtp.port", "587");
        //ssl to starttls
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.auth", "true");

        // Get the Session object.// and pass username and password
        Session session = Session.getInstance(properties, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(email,password);
            }
        });
        // Used to debug SMTP issues
        session.setDebug(true);
        return session;

    }

    /**
     * Sends a plain text email to the specified recipient.
     *
     * @param to      The email address of the recipient.
     * @param subject The subject of the email.
     * @param content The content of the email.
     * @return True if the email is sent successfully; false otherwise.
     */
    public static boolean sendMail(String to, String subject, String content){
        Session session = connect();
        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(email));

            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

            // Set Subject: header field
            message.setSubject(subject);

            // Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();

            // Now set the actual message
            messageBodyPart.setText(content);

            // Create a multipart message
            Multipart multipart = new MimeMultipart();

            // Set text message part
            multipart.addBodyPart(messageBodyPart);

            // Send the complete message parts
            message.setContent(multipart,"UTF-8");

            // Send message
            Transport.send(message);
            return true;
        } catch (MessagingException e) {
            e.printStackTrace();

        }
        return false;
    }

    /**
     * Sends an email with an attachment to the specified recipient.
     *
     * @param to       The email address of the recipient.
     * @param subject  The subject of the email.
     * @param content  The content of the email.
     * @param filePath The file path of the attachment.
     * @return True if the email is sent successfully; false otherwise.
     */
    public static boolean sendMail(String to, String subject, String content, String filePath){
        Session session = connect();
        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(email));

            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

            // Set Subject: header field
            message.setSubject(subject);

            // Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();

            // Now set the actual message
            messageBodyPart.setText(content);

            // Create a multipart message
            Multipart multipart = new MimeMultipart();

            // Set text message part
            multipart.addBodyPart(messageBodyPart);

            // Đính kèm file
            if (filePath != null && !filePath.isEmpty()) {
                messageBodyPart = new MimeBodyPart();
                DataSource source = new FileDataSource(filePath);
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(source.getName());
                multipart.addBodyPart(messageBodyPart);
            }
            // Send the complete message parts
            message.setContent(multipart, "UTF-8");

            // Send message
            Transport.send(message);
            return true;
        } catch (MessagingException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * A sample main method demonstrating how to use the MailService class to send an email with an attachment.
     *
     * @param args The command-line arguments (not used in this example).
     */
    public static void main(String[] args) {
        String email = "20130457@st.hcmuaf.edu.vn";
        String content = "Lỗi rôì. fix đi!";
        SendMail.sendMail(email,"Data Warehouse 2024",content, "D:\\DataWarehouse\\LoadToDataMart");
    }
}
