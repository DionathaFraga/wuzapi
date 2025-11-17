
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
	"github.com/rs/zerolog/log"
)

// ============================================================================
// BOTÕES INTERATIVOS MODERNOS (Buttons Message - Compatível)
// ============================================================================

// SendInteractiveButtons - Envia botões interativos com imagem opcional
func (s *server) SendInteractiveButtons() http.HandlerFunc {
	type Button struct {
		DisplayText string `json:"display_text"`
		ID          string `json:"id"`
	}

	type interactiveButtonRequest struct {
		Phone      string   `json:"Phone"`
		Title      string   `json:"Title"`
		Body       string   `json:"Body"`
		Footer     string   `json:"Footer,omitempty"`
		ImageURL   string   `json:"ImageURL,omitempty"`
		Buttons    []Button `json:"Buttons"`
		Id         string   `json:"Id,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		if clientManager.GetWhatsmeowClient(txtid) == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("no session"))
			return
		}

		var req interactiveButtonRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not decode Payload"))
			return
		}

		// Validações
		if req.Phone == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing Phone in Payload"))
			return
		}

		if req.Title == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing Title in Payload"))
			return
		}

		if req.Body == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing Body in Payload"))
			return
		}

		if len(req.Buttons) < 1 {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing Buttons in Payload (minimum 1)"))
			return
		}

		if len(req.Buttons) > 3 {
			s.Respond(w, r, http.StatusBadRequest, errors.New("maximum 3 buttons allowed"))
			return
		}

		recipient, ok := parseJID(req.Phone)
		if !ok {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not parse Phone"))
			return
		}

		msgid := req.Id
		if msgid == "" {
			msgid = clientManager.GetWhatsmeowClient(txtid).GenerateMessageID()
		}

		// Criar botões usando ButtonsMessage (estrutura compatível)
		var buttons []*waE2E.ButtonsMessage_Button
		for i, btn := range req.Buttons {
			buttons = append(buttons, &waE2E.ButtonsMessage_Button{
				ButtonID:     proto.String(btn.ID),
				ButtonText:   &waE2E.ButtonsMessage_Button_ButtonText{
					DisplayText: proto.String(btn.DisplayText),
				},
				Type: waE2E.ButtonsMessage_Button_RESPONSE.Enum(),
			})
			if i >= 2 { // WhatsApp limita a 3 botões
				break
			}
		}

		// Criar mensagem de botões (formato compatível)
		// Incluindo o título no corpo da mensagem
		bodyText := fmt.Sprintf("*%s*\n\n%s", req.Title, req.Body)
		
		buttonsMsg := &waE2E.ButtonsMessage{
			ContentText: proto.String(bodyText),
			HeaderType:  waE2E.ButtonsMessage_TEXT.Enum(),
			Buttons:     buttons,
		}

		if req.Footer != "" {
			buttonsMsg.FooterText = proto.String(req.Footer)
		}

		msg := &waE2E.Message{
			ButtonsMessage: buttonsMsg,
		}

		resp, err := clientManager.GetWhatsmeowClient(txtid).SendMessage(
			context.Background(),
			recipient,
			msg,
			whatsmeow.SendRequestExtra{ID: msgid},
		)

		if err != nil {
			log.Error().Err(err).Msg("Error sending interactive buttons")
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("error sending message: %v", err)))
			return
		}

		log.Info().Str("timestamp", fmt.Sprintf("%v", resp.Timestamp)).Str("id", msgid).Msg("Interactive buttons sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp.Unix(), "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// ============================================================================
// BOTÕES DE LINK (CTA URL)
// ============================================================================

// SendLinkButton - Envia botão com link usando TemplateMessage
func (s *server) SendLinkButton() http.HandlerFunc {
	type linkButtonRequest struct {
		Phone       string `json:"Phone"`
		Title       string `json:"Title"`
		Body        string `json:"Body"`
		Footer      string `json:"Footer,omitempty"`
		ImageURL    string `json:"ImageURL,omitempty"`
		DisplayText string `json:"DisplayText"`
		URL         string `json:"URL"`
		Id          string `json:"Id,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		if clientManager.GetWhatsmeowClient(txtid) == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("no session"))
			return
		}

		var req linkButtonRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not decode Payload"))
			return
		}

		// Validações
		if req.Phone == "" || req.Title == "" || req.Body == "" || req.DisplayText == "" || req.URL == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing required fields"))
			return
		}

		recipient, ok := parseJID(req.Phone)
		if !ok {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not parse Phone"))
			return
		}

		msgid := req.Id
		if msgid == "" {
			msgid = clientManager.GetWhatsmeowClient(txtid).GenerateMessageID()
		}

		// Criar botão de URL usando TemplateMessage
		urlButton := &waE2E.HydratedTemplateButton{
			Index: proto.Uint32(0),
			HydratedButton: &waE2E.HydratedTemplateButton_UrlButton{
				UrlButton: &waE2E.HydratedTemplateButton_HydratedURLButton{
					DisplayText: proto.String(req.DisplayText),
					URL:         proto.String(req.URL),
				},
			},
		}

		templateMsg := &waE2E.TemplateMessage{
			HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
				HydratedContentText: proto.String(req.Body),
				HydratedFooterText:  proto.String(req.Footer),
				HydratedButtons:     []*waE2E.HydratedTemplateButton{urlButton},
				TemplateID:          proto.String("1"),
			},
		}

		msg := &waE2E.Message{
			TemplateMessage: templateMsg,
		}

		resp, err := clientManager.GetWhatsmeowClient(txtid).SendMessage(
			context.Background(),
			recipient,
			msg,
			whatsmeow.SendRequestExtra{ID: msgid},
		)

		if err != nil {
			log.Error().Err(err).Msg("Error sending link button")
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("error sending message: %v", err)))
			return
		}

		log.Info().Str("timestamp", fmt.Sprintf("%v", resp.Timestamp)).Str("id", msgid).Msg("Link button sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp.Unix(), "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// ============================================================================
// LISTA INTERATIVA
// ============================================================================

// SendInteractiveList - Envia lista interativa usando ListMessage
func (s *server) SendInteractiveList() http.HandlerFunc {
	type ListItem struct {
		Title       string `json:"title"`
		Description string `json:"description,omitempty"`
		ID          string `json:"id"`
	}

	type ListSection struct {
		Title string     `json:"title"`
		Rows  []ListItem `json:"rows"`
	}

	type interactiveListRequest struct {
		Phone        string        `json:"Phone"`
		Title        string        `json:"Title"`
		Body         string        `json:"Body"`
		Footer       string        `json:"Footer,omitempty"`
		ButtonText   string        `json:"ButtonText"`
		ListSections []ListSection `json:"Sections"`
		Id           string        `json:"Id,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		if clientManager.GetWhatsmeowClient(txtid) == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("no session"))
			return
		}

		var req interactiveListRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not decode Payload"))
			return
		}

		// Validações
		if req.Phone == "" || req.Title == "" || req.Body == "" || req.ButtonText == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing required fields"))
			return
		}

		if len(req.ListSections) < 1 {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing sections in Payload"))
			return
		}

		recipient, ok := parseJID(req.Phone)
		if !ok {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not parse Phone"))
			return
		}

		msgid := req.Id
		if msgid == "" {
			msgid = clientManager.GetWhatsmeowClient(txtid).GenerateMessageID()
		}

		// Criar seções da lista usando ListMessage
		var sections []*waE2E.ListMessage_Section
		for _, section := range req.ListSections {
			var rows []*waE2E.ListMessage_Row
			for _, item := range section.Rows {
				rows = append(rows, &waE2E.ListMessage_Row{
					Title:       proto.String(item.Title),
					Description: proto.String(item.Description),
					RowID:       proto.String(item.ID),
				})
			}

			sections = append(sections, &waE2E.ListMessage_Section{
				Title: proto.String(section.Title),
				Rows:  rows,
			})
		}

		listMsg := &waE2E.ListMessage{
			Title:       proto.String(req.Title),
			Description: proto.String(req.Body),
			ButtonText:  proto.String(req.ButtonText),
			ListType:    waE2E.ListMessage_SINGLE_SELECT.Enum(),
			Sections:    sections,
		}

		if req.Footer != "" {
			listMsg.FooterText = proto.String(req.Footer)
		}

		msg := &waE2E.Message{
			ListMessage: listMsg,
		}

		resp, err := clientManager.GetWhatsmeowClient(txtid).SendMessage(
			context.Background(),
			recipient,
			msg,
			whatsmeow.SendRequestExtra{ID: msgid},
		)

		if err != nil {
			log.Error().Err(err).Msg("Error sending interactive list")
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("error sending message: %v", err)))
			return
		}

		log.Info().Str("timestamp", fmt.Sprintf("%v", resp.Timestamp)).Str("id", msgid).Msg("Interactive list sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp.Unix(), "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// ============================================================================
// BOTÃO DE COPIAR (CTA COPY) - Usando CallButton como alternativa
// ============================================================================

// SendCopyButton - Envia botão de copiar código usando TemplateMessage
func (s *server) SendCopyButton() http.HandlerFunc {
	type copyButtonRequest struct {
		Phone       string `json:"Phone"`
		Title       string `json:"Title"`
		Body        string `json:"Body"`
		Footer      string `json:"Footer,omitempty"`
		DisplayText string `json:"DisplayText"`
		CopyCode    string `json:"CopyCode"`
		Id          string `json:"Id,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		if clientManager.GetWhatsmeowClient(txtid) == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("no session"))
			return
		}

		var req copyButtonRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not decode Payload"))
			return
		}

		if req.Phone == "" || req.Title == "" || req.Body == "" || req.DisplayText == "" || req.CopyCode == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing required fields"))
			return
		}

		recipient, ok := parseJID(req.Phone)
		if !ok {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not parse Phone"))
			return
		}

		msgid := req.Id
		if msgid == "" {
			msgid = clientManager.GetWhatsmeowClient(txtid).GenerateMessageID()
		}

		// Criar botão de resposta rápida com o código
		quickReplyButton := &waE2E.HydratedTemplateButton{
			Index: proto.Uint32(0),
			HydratedButton: &waE2E.HydratedTemplateButton_QuickReplyButton{
				QuickReplyButton: &waE2E.HydratedTemplateButton_HydratedQuickReplyButton{
					DisplayText: proto.String(req.DisplayText),
					ID:          proto.String(req.CopyCode),
				},
			},
		}

		bodyText := fmt.Sprintf("%s\n\nCódigo: %s", req.Body, req.CopyCode)

		templateMsg := &waE2E.TemplateMessage{
			HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
				HydratedContentText: proto.String(bodyText),
				HydratedFooterText:  proto.String(req.Footer),
				HydratedButtons:     []*waE2E.HydratedTemplateButton{quickReplyButton},
				TemplateID:          proto.String("1"),
			},
		}

		msg := &waE2E.Message{
			TemplateMessage: templateMsg,
		}

		resp, err := clientManager.GetWhatsmeowClient(txtid).SendMessage(
			context.Background(),
			recipient,
			msg,
			whatsmeow.SendRequestExtra{ID: msgid},
		)

		if err != nil {
			log.Error().Err(err).Msg("Error sending copy button")
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("error sending message: %v", err)))
			return
		}

		log.Info().Str("timestamp", fmt.Sprintf("%v", resp.Timestamp)).Str("id", msgid).Msg("Copy button sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp.Unix(), "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}

// ============================================================================
// BOTÃO DE LIGAR (CTA CALL)
// ============================================================================

// SendCallButton - Envia botão de ligar usando TemplateMessage
func (s *server) SendCallButton() http.HandlerFunc {
	type callButtonRequest struct {
		Phone       string `json:"Phone"`
		Title       string `json:"Title"`
		Body        string `json:"Body"`
		Footer      string `json:"Footer,omitempty"`
		DisplayText string `json:"DisplayText"`
		PhoneNumber string `json:"PhoneNumber"`
		Id          string `json:"Id,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		txtid := r.Context().Value("userinfo").(Values).Get("Id")

		if clientManager.GetWhatsmeowClient(txtid) == nil {
			s.Respond(w, r, http.StatusInternalServerError, errors.New("no session"))
			return
		}

		var req callButtonRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not decode Payload"))
			return
		}

		if req.Phone == "" || req.Title == "" || req.Body == "" || req.DisplayText == "" || req.PhoneNumber == "" {
			s.Respond(w, r, http.StatusBadRequest, errors.New("missing required fields"))
			return
		}

		recipient, ok := parseJID(req.Phone)
		if !ok {
			s.Respond(w, r, http.StatusBadRequest, errors.New("could not parse Phone"))
			return
		}

		msgid := req.Id
		if msgid == "" {
			msgid = clientManager.GetWhatsmeowClient(txtid).GenerateMessageID()
		}

		// Criar botão de chamada
		callButton := &waE2E.HydratedTemplateButton{
			Index: proto.Uint32(0),
			HydratedButton: &waE2E.HydratedTemplateButton_CallButton{
				CallButton: &waE2E.HydratedTemplateButton_HydratedCallButton{
					DisplayText: proto.String(req.DisplayText),
					PhoneNumber: proto.String(req.PhoneNumber),
				},
			},
		}

		templateMsg := &waE2E.TemplateMessage{
			HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
				HydratedContentText: proto.String(req.Body),
				HydratedFooterText:  proto.String(req.Footer),
				HydratedButtons:     []*waE2E.HydratedTemplateButton{callButton},
				TemplateID:          proto.String("1"),
			},
		}

		msg := &waE2E.Message{
			TemplateMessage: templateMsg,
		}

		resp, err := clientManager.GetWhatsmeowClient(txtid).SendMessage(
			context.Background(),
			recipient,
			msg,
			whatsmeow.SendRequestExtra{ID: msgid},
		)

		if err != nil {
			log.Error().Err(err).Msg("Error sending call button")
			s.Respond(w, r, http.StatusInternalServerError, errors.New(fmt.Sprintf("error sending message: %v", err)))
			return
		}

		log.Info().Str("timestamp", fmt.Sprintf("%v", resp.Timestamp)).Str("id", msgid).Msg("Call button sent")
		response := map[string]interface{}{"Details": "Sent", "Timestamp": resp.Timestamp.Unix(), "Id": msgid}
		responseJson, err := json.Marshal(response)
		if err != nil {
			s.Respond(w, r, http.StatusInternalServerError, err)
		} else {
			s.Respond(w, r, http.StatusOK, string(responseJson))
		}
	}
}
