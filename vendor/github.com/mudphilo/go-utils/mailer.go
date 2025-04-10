package library

import (
	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
	"log"
	"os"
)

type Mailer struct {
	to      []string
	subject string
	body    string
}

//NewEmailRequest instanciates mailer object
func NewEmailRequest(to []string, subject, message string) *Mailer {
	return &Mailer{
		to:      to,
		subject: subject,
		body:    message,
	}
}

//Send sends a new email
func (r *Mailer) Send() error {

	from := mail.NewEmail(os.Getenv("EMAIL_FROM_NAME"), os.Getenv("EMAIL_FROM_EMAIL"))

	subject := r.subject
	to := mail.NewEmail(r.to[0], r.to[0])

	message := mail.NewSingleEmail(from, subject, to, r.body, r.body)
	client := sendgrid.NewSendClient(os.Getenv("SENDGRID_API_KEY"))
	resp, _ := client.Send(message)
	log.Printf("response sending email status %d body %s",resp.StatusCode, resp.Body)
	return nil

}
