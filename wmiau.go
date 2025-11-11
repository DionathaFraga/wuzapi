package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/jmoiron/sqlx"
	"github.com/mdp/qrterminal/v3"
	"github.com/patrickmn/go-cache"
	"github.com/rs/zerolog/log"
	"github.com/skip2/go-qrcode"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"golang.org/x/net/proxy"
)

// db field declaration as *sqlx.DB
type MyClient struct {
	WAClient       *whatsmeow.Client
	eventHandlerID uint32
	userID         string
	token          string
	subscriptions  []string
	db             *sqlx.DB
	s              *server
}

// ============================================================================
// LID/JID MANAGEMENT - Sistema completo com PostgreSQL
// ============================================================================

// Estrutura para mapear a tabela whatsmeow_lid_map
type LIDMapping struct {
	JID       string    `db:"jid"`
	LID       string    `db:"lid"`
	PN        string    `db:"pn"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

var jidLidCache = cache.New(24*time.Hour, 1*time.Hour)
var lidCacheMutex sync.RWMutex

func isLID(jid types.JID) bool {
	return jid.Server == "lid"
}

func getRealJID(primary types.JID, alt types.JID) (realJID types.JID, lidJID types.JID) {
	if isLID(primary) {
		return alt, primary
	}
	return primary, alt
}

// Salva LID no banco de dados PostgreSQL
func saveLIDToDB(db *sqlx.DB, jid string, lid string, pn string) error {
	if db == nil {
		log.Error().Msg("❌ Database connection is nil")
		return errors.New("database connection is nil")
	}

	if jid == "" || lid == "" || pn == "" {
		log.Warn().Str("jid", jid).Str("lid", lid).Str("pn", pn).Msg("❌ Valores vazios")
		return errors.New("valores vazios não permitidos")
	}

	query := `
		INSERT INTO public.whatsmeow_lid_map (jid, lid, pn, created_at, updated_at)
		VALUES ($1, $2, $3, NOW(), NOW())
		ON CONFLICT (jid) 
		DO UPDATE SET 
			lid = EXCLUDED.lid,
			pn = EXCLUDED.pn,
			updated_at = NOW()
	`

	_, err := db.Exec(query, jid, lid, pn)
	if err != nil {
		log.Error().Err(err).Str("jid", jid).Str("lid", lid).Msg("❌ Erro ao salvar LID")
		return err
	}

	log.Debug().Str("jid", jid).Str("lid", lid).Str("pn", pn).Msg("✅ LID salvo no banco")
	return nil
}

// Salva LID usando MyClient (versão completa com banco)
func saveLIDForUser(mycli *MyClient, phone string, lid string) {
	if phone == "" || lid == "" {
		log.Warn().Str("userID", mycli.userID).Str("phone", phone).Str("lid", lid).Msg("❌ Valores inválidos")
		return
	}

	jid := fmt.Sprintf("%s@%s", phone, types.DefaultUserServer)
	
	log.Info().Str("userID", mycli.userID).Str("phone", phone).Str("lid", lid).Msg("🔄 Processando LID...")

	// 1. Salvar no cache
	cacheKey := fmt.Sprintf("%s:%s", mycli.userID, lid)
	lidCacheMutex.Lock()
	jidLidCache.Set(cacheKey, phone, cache.DefaultExpiration)
	lidCacheMutex.Unlock()
	log.Debug().Str("cacheKey", cacheKey).Msg("  ✅ Cache atualizado")

	// 2. Salvar no PostgreSQL
	err := saveLIDToDB(mycli.db, jid, lid, phone)
	if err != nil {
		log.Error().Err(err).Msg("  ❌ Falha ao salvar no banco")
		return
	}

	log.Info().Str("userID", mycli.userID).Str("phone", phone).Str("lid", lid).Msg("✅ LID mapeado e persistido!")
}

// Resolve LID para número usando cache + PostgreSQL
func resolveLIDForUser(mycli *MyClient, lid string) (string, bool) {
	if lid == "" {
		return "", false
	}

	log.Debug().Str("userID", mycli.userID).Str("lid", lid).Msg("🔍 Resolvendo LID...")

	// 1. Verificar cache
	cacheKey := fmt.Sprintf("%s:%s", mycli.userID, lid)
	lidCacheMutex.RLock()
	if phone, found := jidLidCache.Get(cacheKey); found {
		lidCacheMutex.RUnlock()
		phoneStr := phone.(string)
		log.Debug().Str("lid", lid).Str("phone", phoneStr).Msg("  ✅ CACHE HIT")
		return phoneStr, true
	}
	lidCacheMutex.RUnlock()

	// 2. Buscar no PostgreSQL
	log.Debug().Str("lid", lid).Msg("  🔎 Buscando no banco...")
	
	var mapping LIDMapping
	query := `SELECT jid, lid, pn FROM public.whatsmeow_lid_map WHERE lid = $1 LIMIT 1`
	
	err := mycli.db.Get(&mapping, query, lid)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Warn().Str("lid", lid).Msg("  ⚠️  LID não encontrado")
		} else {
			log.Error().Err(err).Str("lid", lid).Msg("  ❌ Erro no banco")
		}
		return "", false
	}

	log.Info().Str("lid", lid).Str("pn", mapping.PN).Msg("  ✅ BANCO DE DADOS")

	// 3. Atualizar cache
	lidCacheMutex.Lock()
	jidLidCache.Set(cacheKey, mapping.PN, cache.DefaultExpiration)
	lidCacheMutex.Unlock()
	log.Debug().Str("lid", lid).Msg("  💾 Cache atualizado")

	return mapping.PN, true
}

// Carrega todos LIDs do banco para o cache
func loadLIDsForUser(mycli *MyClient) {
	log.Info().Str("userID", mycli.userID).Msg("📂 Carregando LIDs do banco...")

	var mappings []LIDMapping
	query := `SELECT jid, lid, pn, created_at, updated_at FROM public.whatsmeow_lid_map ORDER BY updated_at DESC`
	
	err := mycli.db.Select(&mappings, query)
	if err != nil {
		log.Error().Err(err).Msg("❌ Erro ao carregar LIDs")
		return
	}

	if len(mappings) == 0 {
		log.Info().Msg("ℹ️  Nenhum LID no banco (primeira execução)")
		return
	}

	// Carregar todos no cache
	lidCacheMutex.Lock()
	for _, mapping := range mappings {
		cacheKey := fmt.Sprintf("%s:%s", mycli.userID, mapping.LID)
		jidLidCache.Set(cacheKey, mapping.PN, cache.DefaultExpiration)
		log.Debug().Str("pn", mapping.PN).Str("lid", mapping.LID).Msg("  🔗 Mapeamento carregado")
	}
	lidCacheMutex.Unlock()

	log.Info().Str("userID", mycli.userID).Int("count", len(mappings)).Msg("✅ LIDs carregados do banco!")
}

// Resolve o JID real a partir de LID
func resolveRealJID(mycli *MyClient, jid types.JID, altJID types.JID) types.JID {
	if !isLID(jid) {
		return jid
	}

	log.Debug().Str("jid_user", jid.User).Str("jid_server", jid.Server).Msg("🔄 Resolvendo JID real de LID...")

	if altJID.Server != "" && !isLID(altJID) {
		saveLIDForUser(mycli, altJID.User, jid.User)
		log.Info().Str("real_jid", altJID.User).Str("lid", jid.User).Msg("  ✅ Usando altJID")
		return altJID
	}

	if phone, found := resolveLIDForUser(mycli, jid.User); found {
		realJID := types.NewJID(phone, types.DefaultUserServer)
		log.Info().Str("phone", phone).Str("lid", jid.User).Msg("  ✅ LID resolvido")
		return realJID
	}

	log.Warn().Str("lid", jid.User).Msg("  ⚠️  LID não resolvido, retornando original")
	return types.NewJID(jid.User, types.DefaultUserServer)
}

// Identifica tipo de chat e resolve LIDs
func identifyChatType(mycli *MyClient, chatJID types.JID, senderJID types.JID, senderAlt types.JID) (bool, types.JID, types.JID) {
	log.Debug().
		Str("userID", mycli.userID).
		Str("chatJID", chatJID.String()).
		Str("senderJID", senderJID.String()).
		Str("senderAlt", senderAlt.String()).
		Msg("🔍 Identificando tipo de chat...")

	isGroup := chatJID.Server == types.GroupServer || 
	           chatJID.Server == types.BroadcastServer || 
	           strings.Contains(chatJID.Server, "broadcast")

	if isGroup {
		log.Debug().Str("chatJID", chatJID.String()).Msg("  👥 GRUPO detectado")
		realChatJID := chatJID
		senderReal, senderLID := getRealJID(senderJID, senderAlt)
		
		if isLID(senderLID) {
			log.Info().
				Str("senderReal", senderReal.User).
				Str("senderLID", senderLID.User).
				Msg("  🆔 LID detectado em GRUPO - salvando...")
			saveLIDForUser(mycli, senderReal.User, senderLID.User)
		} else {
			log.Debug().Str("senderReal", senderReal.User).Msg("  ✓ Sender não é LID")
		}
		
		return true, realChatJID, senderReal
	} else {
		log.Debug().Str("chatJID", chatJID.String()).Msg("  👤 CHAT INDIVIDUAL detectado")
		realChatJID := resolveRealJID(mycli, chatJID, senderAlt)
		senderReal, senderLID := getRealJID(senderJID, senderAlt)
		
		if isLID(senderLID) {
			log.Info().
				Str("senderReal", senderReal.User).
				Str("senderLID", senderLID.User).
				Msg("  🆔 LID detectado em CHAT INDIVIDUAL - salvando...")
			saveLIDForUser(mycli, senderReal.User, senderLID.User)
		} else {
			log.Debug().Str("senderReal", senderReal.User).Msg("  ✓ Sender não é LID")
		}
		
		log.Debug().
			Str("realChatJID", realChatJID.String()).
			Str("senderReal", senderReal.String()).
			Msg("  ✅ Chat identificado e LIDs resolvidos")
		
		return false, realChatJID, senderReal
	}
}

// ============================================================================
// RESTO DO CÓDIGO CONTINUA IGUAL AO SEU ORIGINAL
// ============================================================================

func sendToGlobalWebHook(jsonData []byte, token string, userID string) {
	jsonDataStr := string(jsonData)

	instance_name := ""
	userinfo, found := userinfocache.Get(token)
	if found {
		instance_name = userinfo.(Values).Get("Name")
	}

	if *globalWebhook != "" {
		log.Info().Str("url", *globalWebhook).Msg("Calling global webhook")
		globalData := map[string]string{
			"jsonData":     jsonDataStr,
			"userID":       userID,
			"instanceName": instance_name,
		}
		callHookWithHmac(*globalWebhook, globalData, userID, globalHMACKeyEncrypted)
	}
}

func sendToUserWebHook(webhookurl string, path string, jsonData []byte, userID string, token string) {
	sendToUserWebHookWithHmac(webhookurl, path, jsonData, userID, token, nil)
}

func sendToUserWebHookWithHmac(webhookurl string, path string, jsonData []byte, userID string, token string, encryptedHmacKey []byte) {
	instance_name := ""
	userinfo, found := userinfocache.Get(token)
	if found {
		instance_name = userinfo.(Values).Get("Name")
	}
	data := map[string]string{
		"jsonData":     string(jsonData),
		"userID":       userID,
		"instanceName": instance_name,
	}

	log.Debug().Interface("webhookData", data).Msg("Data being sent to webhook")

	if webhookurl != "" {
		log.Info().Str("url", webhookurl).Msg("Calling user webhook")

		if path == "" {
			go callHookWithHmac(webhookurl, data, userID, encryptedHmacKey)
		} else {
			errChan := make(chan error, 1)
			go func() {
				err := callHookFileWithHmac(webhookurl, data, userID, path, encryptedHmacKey)
				errChan <- err
			}()

			if err := <-errChan; err != nil {
				log.Error().Err(err).Msg("Error calling hook file")
			}
		}
	} else {
		log.Warn().Str("userid", userID).Msg("No webhook set for user")
	}
}

func updateAndGetUserSubscriptions(mycli *MyClient) ([]string, error) {
	currentEvents := ""
	userinfo2, found2 := userinfocache.Get(mycli.token)
	if found2 {
		currentEvents = userinfo2.(Values).Get("Events")
	} else {
		if err := mycli.db.Get(&currentEvents, "SELECT events FROM users WHERE id=$1", mycli.userID); err != nil {
			log.Warn().Err(err).Str("userID", mycli.userID).Msg("Could not get events from DB")
			return nil, err
		}
	}

	eventarray := strings.Split(currentEvents, ",")
	var subscribedEvents []string
	if len(eventarray) == 1 && eventarray[0] == "" {
		subscribedEvents = []string{}
	} else {
		for _, arg := range eventarray {
			arg = strings.TrimSpace(arg)
			if arg != "" && Find(supportedEventTypes, arg) {
				subscribedEvents = append(subscribedEvents, arg)
			}
		}
	}

	mycli.subscriptions = subscribedEvents
	return subscribedEvents, nil
}

func getUserWebhookUrl(token string) string {
	webhookurl := ""
	myuserinfo, found := userinfocache.Get(token)
	if !found {
		log.Warn().Str("token", token).Msg("Could not call webhook as there is no user for this token")
	} else {
		webhookurl = myuserinfo.(Values).Get("Webhook")
	}
	return webhookurl
}

func sendEventWithWebHook(mycli *MyClient, postmap map[string]interface{}, path string) {
	webhookurl := getUserWebhookUrl(mycli.token)

	subscribedEvents, err := updateAndGetUserSubscriptions(mycli)
	if err != nil {
		return
	}

	eventType, ok := postmap["type"].(string)
	if !ok {
		log.Error().Msg("Event type is not a string in postmap")
		return
	}

	log.Debug().
		Str("userID", mycli.userID).
		Str("eventType", eventType).
		Strs("subscribedEvents", subscribedEvents).
		Msg("Checking event subscription")

	checkIfSubscribedInEvent := checkIfSubscribedToEvent(subscribedEvents, postmap["type"].(string), mycli.userID)
	if !checkIfSubscribedInEvent {
		return
	}

	jsonData, err := json.Marshal(postmap)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal postmap to JSON")
		return
	}

	var encryptedHmacKey []byte
	if userinfo, found := userinfocache.Get(mycli.token); found {
		encryptedB64 := userinfo.(Values).Get("HmacKeyEncrypted")
		if encryptedB64 != "" {
			var err error
			encryptedHmacKey, err = base64.StdEncoding.DecodeString(encryptedB64)
			if err != nil {
				log.Error().Err(err).Msg("Failed to decode HMAC key from cache")
			}
		}
	}

	sendToUserWebHookWithHmac(webhookurl, path, jsonData, mycli.userID, mycli.token, encryptedHmacKey)
	go sendToGlobalWebHook(jsonData, mycli.token, mycli.userID)
	go sendToGlobalRabbit(jsonData, mycli.token, mycli.userID)
}

func checkIfSubscribedToEvent(subscribedEvents []string, eventType string, userId string) bool {
	if !Find(subscribedEvents, eventType) && !Find(subscribedEvents, "All") {
		log.Warn().
			Str("type", eventType).
			Strs("subscribedEvents", subscribedEvents).
			Str("userID", userId).
			Msg("Skipping webhook. Not subscribed for this type")
		return false
	}
	return true
}

func (s *server) connectOnStartup() {
	rows, err := s.db.Queryx("SELECT id,name,token,jid,webhook,events,proxy_url,CASE WHEN s3_enabled THEN 'true' ELSE 'false' END AS s3_enabled,media_delivery,COALESCE(history, 0) as history,hmac_key FROM users WHERE connected=1")
	if err != nil {
		log.Error().Err(err).Msg("DB Problem")
		return
	}
	defer rows.Close()
	for rows.Next() {
		txtid := ""
		token := ""
		jid := ""
		name := ""
		webhook := ""
		events := ""
		proxy_url := ""
		s3_enabled := ""
		media_delivery := ""
		var history int
		var hmac_key []byte
		err = rows.Scan(&txtid, &name, &token, &jid, &webhook, &events, &proxy_url, &s3_enabled, &media_delivery, &history, &hmac_key)
		if err != nil {
			log.Error().Err(err).Msg("DB Problem")
			return
		} else {
			hmacKeyEncrypted := ""
			if len(hmac_key) > 0 {
				hmacKeyEncrypted = base64.StdEncoding.EncodeToString(hmac_key)
			}

			log.Info().Str("token", token).Msg("Connect to Whatsapp on startup")
			v := Values{map[string]string{
				"Id":               txtid,
				"Name":             name,
				"Jid":              jid,
				"Webhook":          webhook,
				"Token":            token,
				"Proxy":            proxy_url,
				"Events":           events,
				"S3Enabled":        s3_enabled,
				"MediaDelivery":    media_delivery,
				"History":          fmt.Sprintf("%d", history),
				"HmacKeyEncrypted": hmacKeyEncrypted,
			}}
			userinfocache.Set(token, v, cache.NoExpiration)
			eventarray := strings.Split(events, ",")

			var subscribedEvents []string
			if len(eventarray) == 1 && eventarray[0] == "" {
				subscribedEvents = []string{}
			} else {
				for _, arg := range eventarray {
					if !Find(supportedEventTypes, arg) {
						log.Warn().Str("Type", arg).Msg("Event type discarded")
						continue
					}
					if !Find(subscribedEvents, arg) {
						subscribedEvents = append(subscribedEvents, arg)
					}
				}

			}
			eventstring := strings.Join(subscribedEvents, ",")
			log.Info().Str("events", eventstring).Str("jid", jid).Msg("Attempt to connect")
			killchannel[txtid] = make(chan bool)
			go s.startClient(txtid, jid, token, subscribedEvents)

			go func(userID string) {
				var s3Config struct {
					Enabled       bool   `db:"s3_enabled"`
					Endpoint      string `db:"s3_endpoint"`
					Region        string `db:"s3_region"`
					Bucket        string `db:"s3_bucket"`
					AccessKey     string `db:"s3_access_key"`
					SecretKey     string `db:"s3_secret_key"`
					PathStyle     bool   `db:"s3_path_style"`
					PublicURL     string `db:"s3_public_url"`
					RetentionDays int    `db:"s3_retention_days"`
				}

				err := s.db.Get(&s3Config, `
					SELECT s3_enabled, s3_endpoint, s3_region, s3_bucket, 
						   s3_access_key, s3_secret_key, s3_path_style, 
						   s3_public_url, s3_retention_days
					FROM users WHERE id = $1`, userID)

				if err != nil {
					log.Error().Err(err).Str("userID", userID).Msg("Failed to get S3 config")
					return
				}

				if s3Config.Enabled {
					config := &S3Config{
						Enabled:       s3Config.Enabled,
						Endpoint:      s3Config.Endpoint,
						Region:        s3Config.Region,
						Bucket:        s3Config.Bucket,
						AccessKey:     s3Config.AccessKey,
						SecretKey:     s3Config.SecretKey,
						PathStyle:     s3Config.PathStyle,
						PublicURL:     s3Config.PublicURL,
						RetentionDays: s3Config.RetentionDays,
					}

					err = GetS3Manager().InitializeS3Client(userID, config)
					if err != nil {
						log.Error().Err(err).Str("userID", userID).Msg("Failed to initialize S3 client on startup")
					} else {
						log.Info().Str("userID", userID).Msg("S3 client initialized on startup")
					}
				}
			}(txtid)
		}
	}
	err = rows.Err()
	if err != nil {
		log.Error().Err(err).Msg("DB Problem")
	}
}

func parseJID(arg string) (types.JID, bool) {
	if arg[0] == '+' {
		arg = arg[1:]
	}
	if !strings.ContainsRune(arg, '@') {
		return types.NewJID(arg, types.DefaultUserServer), true
	} else {
		recipient, err := types.ParseJID(arg)
		if err != nil {
			log.Error().Err(err).Msg("Invalid JID")
			return recipient, false
		} else if recipient.User == "" {
			log.Error().Err(err).Msg("Invalid JID no server specified")
			return recipient, false
		}
		return recipient, true
	}
}

func (s *server) startClient(userID string, textjid string, token string, subscriptions []string) {
	log.Info().Str("userid", userID).Str("jid", textjid).Msg("Starting websocket connection to Whatsapp")

	const maxConnectionRetries = 3
	const connectionRetryBaseWait = 5 * time.Second

	var deviceStore *store.Device
	var err error

	if textjid != "" {
		jid, _ := parseJID(textjid)
		deviceStore, err = container.GetDevice(context.Background(), jid)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get device")
			deviceStore = container.NewDevice()
		}
	} else {
		log.Warn().Msg("No jid found. Creating new device")
		deviceStore = container.NewDevice()
	}

	if deviceStore == nil {
		log.Warn().Msg("No store found. Creating new one")
		deviceStore = container.NewDevice()
	}

	clientLog := waLog.Stdout("Client", *waDebug, *colorOutput)

	var client *whatsmeow.Client
	if *waDebug != "" {
		client = whatsmeow.NewClient(deviceStore, clientLog)
	} else {
		client = whatsmeow.NewClient(deviceStore, nil)
	}

	clientManager.SetWhatsmeowClient(userID, client)

	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_UNKNOWN.Enum()
	store.DeviceProps.Os = osName

	mycli := MyClient{client, 1, userID, token, subscriptions, s.db, s}
	mycli.eventHandlerID = mycli.WAClient.AddEventHandler(mycli.myEventHandler)

	clientManager.SetMyClient(userID, &mycli)

	// ✅ CARREGAR LIDs DO BANCO NA INICIALIZAÇÃO
	loadLIDsForUser(&mycli)

	httpClient := resty.New()
	httpClient.SetRedirectPolicy(resty.FlexibleRedirectPolicy(15))
	if *waDebug == "DEBUG" {
		httpClient.SetDebug(true)
	}
	httpClient.SetTimeout(30 * time.Second)
	httpClient.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	httpClient.OnError(func(req *resty.Request, err error) {
		if v, ok := err.(*resty.ResponseError); ok {
			log.Debug().Str("response", v.Response.String()).Msg("resty error")
			log.Error().Err(v.Err).Msg("resty error")
		}
	})

	var proxyURL string
	err = s.db.Get(&proxyURL, "SELECT proxy_url FROM users WHERE id=$1", userID)
	if err == nil && proxyURL != "" {
		parsed, perr := url.Parse(proxyURL)
		if perr != nil {
			log.Warn().Err(perr).Str("proxy", proxyURL).Msg("Invalid proxy URL, skipping proxy setup")
		} else {
			log.Info().Str("proxy", proxyURL).Msg("Configuring proxy")

			if parsed.Scheme == "socks5" || parsed.Scheme == "socks5h" {
				dialer, derr := proxy.FromURL(parsed, nil)
				if derr != nil {
					log.Warn().Err(derr).Str("proxy", proxyURL).Msg("Failed to build SOCKS proxy dialer")
				} else {
					httpClient.SetProxy(proxyURL)
					client.SetSOCKSProxy(dialer, whatsmeow.SetProxyOptions{})
					log.Info().Msg("SOCKS proxy configured successfully")
				}
			} else {
				httpClient.SetProxy(proxyURL)
				client.SetProxyAddress(parsed.String(), whatsmeow.SetProxyOptions{})
				log.Info().Msg("HTTP/HTTPS proxy configured successfully")
			}
		}
	}
	clientManager.SetHTTPClient(userID, httpClient)

	if client.Store.ID == nil {
		qrChan, err := client.GetQRChannel(context.Background())
		if err != nil {
			if !errors.Is(err, whatsmeow.ErrQRStoreContainsID) {
				log.Error().Err(err).Msg("Failed to get QR channel")
				return
			}
		} else {
			err = client.Connect()
			if err != nil {
				log.Error().Err(err).Msg("Failed to connect client")
				return
			}

			myuserinfo, found := userinfocache.Get(token)

			for evt := range qrChan {
				if evt.Event == "code" {
					if *logType != "json" {
						qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
						fmt.Println("QR code:\n", evt.Code)
					}
					image, _ := qrcode.Encode(evt.Code, qrcode.Medium, 256)
					base64qrcode := "data:image/png;base64," + base64.StdEncoding.EncodeToString(image)
					sqlStmt := `UPDATE users SET qrcode=$1 WHERE id=$2`
					_, err := s.db.Exec(sqlStmt, base64qrcode, userID)
					if err != nil {
						log.Error().Err(err).Msg(sqlStmt)
					} else {
						if found {
							v := updateUserInfo(myuserinfo, "Qrcode", base64qrcode)
							userinfocache.Set(token, v, cache.NoExpiration)
							log.Info().Str("qrcode", base64qrcode).Msg("update cache userinfo with qr code")
						}
					}

					postmap := make(map[string]interface{})
					postmap["event"] = evt.Event
					postmap["qrCodeBase64"] = base64qrcode
					postmap["type"] = "QR"

					sendEventWithWebHook(&mycli, postmap, "")

				} else if evt.Event == "timeout" {
					postmap := make(map[string]interface{})
					postmap["event"] = evt.Event
					postmap["type"] = "QRTimeout"
					sendEventWithWebHook(&mycli, postmap, "")

					sqlStmt := `UPDATE users SET qrcode='' WHERE id=$1`
					_, err := s.db.Exec(sqlStmt, userID)
					if err != nil {
						log.Error().Err(err).Msg(sqlStmt)
					} else {
						if found {
							v := updateUserInfo(myuserinfo, "Qrcode", "")
							userinfocache.Set(token, v, cache.NoExpiration)
						}
					}
					log.Warn().Msg("QR timeout killing channel")
					clientManager.DeleteWhatsmeowClient(userID)
					clientManager.DeleteMyClient(userID)
					clientManager.DeleteHTTPClient(userID)
					killchannel[userID] <- true
				} else if evt.Event == "success" {
					log.Info().Msg("QR pairing ok!")
					sqlStmt := `UPDATE users SET qrcode='', connected=1 WHERE id=$1`
					_, err := s.db.Exec(sqlStmt, userID)
					if err != nil {
						log.Error().Err(err).Msg(sqlStmt)
					} else {
						if found {
							v := updateUserInfo(myuserinfo, "Qrcode", "")
							userinfocache.Set(token, v, cache.NoExpiration)
						}
					}
				} else {
					log.Info().Str("event", evt.Event).Msg("Login event")
				}
			}
		}

	} else {
		log.Info().Msg("Already logged in, just connect")

		var lastErr error

		for attempt := 0; attempt < maxConnectionRetries; attempt++ {
			if attempt > 0 {
				waitTime := time.Duration(attempt) * connectionRetryBaseWait
				log.Warn().
					Int("attempt", attempt+1).
					Int("max_retries", maxConnectionRetries).
					Dur("wait_time", waitTime).
					Msg("Retrying connection after delay")
				time.Sleep(waitTime)
			}

			err = client.Connect()
			if err == nil {
				log.Info().
					Int("attempt", attempt+1).
					Msg("Successfully connected to WhatsApp")
				break
			}

			lastErr = err
			log.Warn().
				Err(err).
				Int("attempt", attempt+1).
				Int("max_retries", maxConnectionRetries).
				Msg("Failed to connect to WhatsApp")
		}

		if lastErr != nil {
			log.Error().
				Err(lastErr).
				Str("userid", userID).
				Int("attempts", maxConnectionRetries).
				Msg("Failed to connect to WhatsApp after all retry attempts")

			clientManager.DeleteWhatsmeowClient(userID)
			clientManager.DeleteMyClient(userID)
			clientManager.DeleteHTTPClient(userID)

			sqlStmt := `UPDATE users SET qrcode='', connected=0 WHERE id=$1`
			_, dbErr := s.db.Exec(sqlStmt, userID)
			if dbErr != nil {
				log.Error().Err(dbErr).Msg("Failed to update user status after connection error")
			}

			postmap := make(map[string]interface{})
			postmap["event"] = "ConnectFailure"
			postmap["error"] = lastErr.Error()
			postmap["type"] = "ConnectFailure"
			postmap["attempts"] = maxConnectionRetries
			postmap["reason"] = "Failed to connect after retry attempts"
			sendEventWithWebHook(&mycli, postmap, "")

			return
		}
	}

	for {
		select {
		case <-killchannel[userID]:
			log.Info().Str("userid", userID).Msg("Received kill signal")
			client.Disconnect()
			clientManager.DeleteWhatsmeowClient(userID)
			clientManager.DeleteMyClient(userID)
			clientManager.DeleteHTTPClient(userID)
			sqlStmt := `UPDATE users SET qrcode='', connected=0 WHERE id=$1`
			_, err := s.db.Exec(sqlStmt, userID)
			if err != nil {
				log.Error().Err(err).Msg(sqlStmt)
			}
			return
		default:
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func fileToBase64(filepath string) (string, string, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return "", "", err
	}
	mimeType := http.DetectContentType(data)
	return base64.StdEncoding.EncodeToString(data), mimeType, nil
}

func (mycli *MyClient) myEventHandler(rawEvt interface{}) {
	txtid := mycli.userID
	postmap := make(map[string]interface{})
	postmap["event"] = rawEvt
	dowebhook := 0
	path := ""

	switch evt := rawEvt.(type) {
	case *events.AppStateSyncComplete:
		if len(mycli.WAClient.Store.PushName) > 0 && evt.Name == appstate.WAPatchCriticalBlock {
			err := mycli.WAClient.SendPresence(context.Background(), types.PresenceAvailable)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to send available presence")
			} else {
				log.Info().Msg("Marked self as available")
			}
		}
	case *events.Connected, *events.PushNameSetting:
		postmap["type"] = "Connected"
		dowebhook = 1
		if len(mycli.WAClient.Store.PushName) == 0 {
			break
		}
		err := mycli.WAClient.SendPresence(context.Background(), types.PresenceAvailable)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to send available presence")
		} else {
			log.Info().Msg("Marked self as available")
		}
		sqlStmt := `UPDATE users SET connected=1 WHERE id=$1`
		_, err = mycli.db.Exec(sqlStmt, mycli.userID)
		if err != nil {
			log.Error().Err(err).Msg(sqlStmt)
			return
		}
	case *events.PairSuccess:
		log.Info().Str("userid", mycli.userID).Str("token", mycli.token).Str("ID", evt.ID.String()).Str("BusinessName", evt.BusinessName).Str("Platform", evt.Platform).Msg("QR Pair Success")
		jid := evt.ID
		sqlStmt := `UPDATE users SET jid=$1 WHERE id=$2`
		_, err := mycli.db.Exec(sqlStmt, jid, mycli.userID)
		if err != nil {
			log.Error().Err(err).Msg(sqlStmt)
			return
		}

		myuserinfo, found := userinfocache.Get(mycli.token)
		if !found {
			log.Warn().Msg("No user info cached on pairing?")
		} else {
			txtid = myuserinfo.(Values).Get("Id")
			token := myuserinfo.(Values).Get("Token")
			v := updateUserInfo(myuserinfo, "Jid", fmt.Sprintf("%s", jid))
			userinfocache.Set(token, v, cache.NoExpiration)
			log.Info().Str("jid", jid.String()).Str("userid", txtid).Str("token", token).Msg("User information set")
		}
	case *events.StreamReplaced:
		log.Info().Msg("Received StreamReplaced event")
		return
	case *events.Message:

		var s3Config struct {
			Enabled       string `db:"s3_enabled"`
			MediaDelivery string `db:"media_delivery"`
		}

		lastMessageCache.Set(mycli.userID, &evt.Info, cache.DefaultExpiration)
		myuserinfo, found := userinfocache.Get(mycli.token)
		if !found {
			err := mycli.db.Get(&s3Config, "SELECT CASE WHEN s3_enabled = 1 THEN 'true' ELSE 'false' END AS s3_enabled, media_delivery FROM users WHERE id = $1", txtid)
			if err != nil {
				log.Error().Err(err).Msg("onMessage Failed to get S3 config from DB")
				s3Config.Enabled = "false"
				s3Config.MediaDelivery = "base64"
			}
		} else {
			s3Config.Enabled = myuserinfo.(Values).Get("S3Enabled")
			s3Config.MediaDelivery = myuserinfo.(Values).Get("MediaDelivery")
		}

		postmap["type"] = "Message"
		dowebhook = 1
		metaParts := []string{fmt.Sprintf("pushname: %s", evt.Info.PushName), fmt.Sprintf("timestamp: %s", evt.Info.Timestamp)}
		if evt.Info.Type != "" {
			metaParts = append(metaParts, fmt.Sprintf("type: %s", evt.Info.Type))
		}
		if evt.Info.Category != "" {
			metaParts = append(metaParts, fmt.Sprintf("category: %s", evt.Info.Category))
		}
		if evt.IsViewOnce {
			metaParts = append(metaParts, "view once")
		}
		if evt.IsEphemeral {
			metaParts = append(metaParts, "ephemeral")
		}

		log.Info().Str("id", evt.Info.ID).Str("source", evt.Info.SourceString()).Str("parts", strings.Join(metaParts, ", ")).Msg("Message Received")

		if !*skipMedia {
			img := evt.Message.GetImageMessage()
			if img != nil {
				tmpDirectory := filepath.Join("/tmp", "user_"+txtid)
				errDir := os.MkdirAll(tmpDirectory, 0751)
				if errDir != nil {
					log.Error().Err(errDir).Msg("Could not create temporary directory")
					return
				}

				data, err := mycli.WAClient.Download(context.Background(), img)
				if err != nil {
					log.Error().Err(err).Msg("Failed to download image")
					return
				}

				exts, _ := mime.ExtensionsByType(img.GetMimetype())
				tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+exts[0])

				err = os.WriteFile(tmpPath, data, 0600)
				if err != nil {
					log.Error().Err(err).Msg("Failed to save image to temporary file")
					return
				}

				if s3Config.Enabled == "true" && (s3Config.MediaDelivery == "s3" || s3Config.MediaDelivery == "both") {
					isIncoming := evt.Info.IsFromMe == false
					isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
					contactJID := realSenderJID.String()
					if isGroup {
						contactJID = realChatJID.String()
					}

					s3Data, err := GetS3Manager().ProcessMediaForS3(
						context.Background(),
						txtid,
						contactJID,
						evt.Info.ID,
						data,
						img.GetMimetype(),
						filepath.Base(tmpPath),
						isIncoming,
					)
					if err != nil {
						log.Error().Err(err).Msg("Failed to upload image to S3")
					} else {
						postmap["s3"] = s3Data
					}
				}

				if s3Config.MediaDelivery == "base64" || s3Config.MediaDelivery == "both" {
					base64String, mimeType, err := fileToBase64(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to convert image to base64")
						return
					}

					postmap["base64"] = base64String
					postmap["mimeType"] = mimeType
					postmap["fileName"] = filepath.Base(tmpPath)
				}

				log.Info().Str("path", tmpPath).Msg("Image processed")

				err = os.Remove(tmpPath)
				if err != nil {
					log.Error().Err(err).Msg("Failed to delete temporary file")
				} else {
					log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
				}
			}

			audio := evt.Message.GetAudioMessage()
			if audio != nil {
				tmpDirectory := filepath.Join("/tmp", "user_"+txtid)
				errDir := os.MkdirAll(tmpDirectory, 0751)
				if errDir != nil {
					log.Error().Err(errDir).Msg("Could not create temporary directory")
					return
				}

				data, err := mycli.WAClient.Download(context.Background(), audio)
				if err != nil {
					log.Error().Err(err).Msg("Failed to download audio")
					return
				}

				exts, _ := mime.ExtensionsByType(audio.GetMimetype())
				var ext string
				if len(exts) > 0 {
					ext = exts[0]
				} else {
					ext = ".ogg"
				}
				tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+ext)

				err = os.WriteFile(tmpPath, data, 0600)
				if err != nil {
					log.Error().Err(err).Msg("Failed to save audio to temporary file")
					return
				}

				if s3Config.Enabled == "true" && (s3Config.MediaDelivery == "s3" || s3Config.MediaDelivery == "both") {
					isIncoming := evt.Info.IsFromMe == false
					isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
					contactJID := realSenderJID.String()
					if isGroup {
						contactJID = realChatJID.String()
					}

					s3Data, err := GetS3Manager().ProcessMediaForS3(
						context.Background(),
						txtid,
						contactJID,
						evt.Info.ID,
						data,
						audio.GetMimetype(),
						filepath.Base(tmpPath),
						isIncoming,
					)
					if err != nil {
						log.Error().Err(err).Msg("Failed to upload audio to S3")
					} else {
						postmap["s3"] = s3Data
					}
				}

				if s3Config.MediaDelivery == "base64" || s3Config.MediaDelivery == "both" {
					base64String, mimeType, err := fileToBase64(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to convert audio to base64")
						return
					}

					postmap["base64"] = base64String
					postmap["mimeType"] = mimeType
					postmap["fileName"] = filepath.Base(tmpPath)
				}

				log.Info().Str("path", tmpPath).Msg("Audio processed")

				err = os.Remove(tmpPath)
				if err != nil {
					log.Error().Err(err).Msg("Failed to delete temporary file")
				} else {
					log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
				}
			}

			document := evt.Message.GetDocumentMessage()
			if document != nil {
				tmpDirectory := filepath.Join("/tmp", "user_"+txtid)
				errDir := os.MkdirAll(tmpDirectory, 0751)
				if errDir != nil {
					log.Error().Err(errDir).Msg("Could not create temporary directory")
					return
				}

				data, err := mycli.WAClient.Download(context.Background(), document)
				if err != nil {
					log.Error().Err(err).Msg("Failed to download document")
					return
				}

				extension := ""
				exts, err := mime.ExtensionsByType(document.GetMimetype())
				if err == nil && len(exts) > 0 {
					extension = exts[0]
				} else {
					filename := document.FileName
					if filename != nil {
						extension = filepath.Ext(*filename)
					} else {
						extension = ".bin"
					}
				}
				tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+extension)

				err = os.WriteFile(tmpPath, data, 0600)
				if err != nil {
					log.Error().Err(err).Msg("Failed to save document to temporary file")
					return
				}

				if s3Config.Enabled == "true" && (s3Config.MediaDelivery == "s3" || s3Config.MediaDelivery == "both") {
					isIncoming := evt.Info.IsFromMe == false
					isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
					contactJID := realSenderJID.String()
					if isGroup {
						contactJID = realChatJID.String()
					}

					s3Data, err := GetS3Manager().ProcessMediaForS3(
						context.Background(),
						txtid,
						contactJID,
						evt.Info.ID,
						data,
						document.GetMimetype(),
						filepath.Base(tmpPath),
						isIncoming,
					)
					if err != nil {
						log.Error().Err(err).Msg("Failed to upload document to S3")
					} else {
						postmap["s3"] = s3Data
					}
				}

				if s3Config.MediaDelivery == "base64" || s3Config.MediaDelivery == "both" {
					base64String, mimeType, err := fileToBase64(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to convert document to base64")
						return
					}

					postmap["base64"] = base64String
					postmap["mimeType"] = mimeType
					postmap["fileName"] = filepath.Base(tmpPath)
				}

				log.Info().Str("path", tmpPath).Msg("Document processed")

				err = os.Remove(tmpPath)
				if err != nil {
					log.Error().Err(err).Msg("Failed to delete temporary file")
				} else {
					log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
				}
			}

			video := evt.Message.GetVideoMessage()
			if video != nil {
				tmpDirectory := filepath.Join("/tmp", "user_"+txtid)
				errDir := os.MkdirAll(tmpDirectory, 0751)
				if errDir != nil {
					log.Error().Err(errDir).Msg("Could not create temporary directory")
					return
				}

				data, err := mycli.WAClient.Download(context.Background(), video)
				if err != nil {
					log.Error().Err(err).Msg("Failed to download video")
					return
				}

				exts, _ := mime.ExtensionsByType(video.GetMimetype())
				tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+exts[0])

				err = os.WriteFile(tmpPath, data, 0600)
				if err != nil {
					log.Error().Err(err).Msg("Failed to save video to temporary file")
					return
				}

				if s3Config.Enabled == "true" && (s3Config.MediaDelivery == "s3" || s3Config.MediaDelivery == "both") {
					isIncoming := evt.Info.IsFromMe == false
					isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
					contactJID := realSenderJID.String()
					if isGroup {
						contactJID = realChatJID.String()
					}

					s3Data, err := GetS3Manager().ProcessMediaForS3(
						context.Background(),
						txtid,
						contactJID,
						evt.Info.ID,
						data,
						video.GetMimetype(),
						filepath.Base(tmpPath),
						isIncoming,
					)
					if err != nil {
						log.Error().Err(err).Msg("Failed to upload video to S3")
					} else {
						postmap["s3"] = s3Data
					}
				}

				if s3Config.MediaDelivery == "base64" || s3Config.MediaDelivery == "both" {
					base64String, mimeType, err := fileToBase64(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to convert video to base64")
						return
					}

					postmap["base64"] = base64String
					postmap["mimeType"] = mimeType
					postmap["fileName"] = filepath.Base(tmpPath)
				}

				log.Info().Str("path", tmpPath).Msg("Video processed")

				err = os.Remove(tmpPath)
				if err != nil {
					log.Error().Err(err).Msg("Failed to delete temporary file")
				} else {
					log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
				}
			}

			sticker := evt.Message.GetStickerMessage()
			if sticker != nil {
				tmpDirectory := filepath.Join("/tmp", "user_"+txtid)
				errDir := os.MkdirAll(tmpDirectory, 0751)
				if errDir != nil {
					log.Error().Err(errDir).Msg("Could not create temporary directory")
					return
				}

				data, err := mycli.WAClient.Download(context.Background(), sticker)
				if err != nil {
					log.Error().Err(err).Msg("Failed to download sticker")
					return
				}

				exts, _ := mime.ExtensionsByType(sticker.GetMimetype())
				ext := ".webp"
				if len(exts) > 0 && exts[0] != "" {
					ext = exts[0]
				}

				tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+ext)
				if err := os.WriteFile(tmpPath, data, 0600); err != nil {
					log.Error().Err(err).Msg("Failed to save sticker to temporary file")
					return
				}

				if s3Config.Enabled == "true" && (s3Config.MediaDelivery == "s3" || s3Config.MediaDelivery == "both") {
					isIncoming := evt.Info.IsFromMe == false
					isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
					contactJID := realSenderJID.String()
					if isGroup {
						contactJID = realChatJID.String()
					}
					s3Data, err := GetS3Manager().ProcessMediaForS3(
						context.Background(),
						txtid,
						contactJID,
						evt.Info.ID,
						data,
						sticker.GetMimetype(),
						filepath.Base(tmpPath),
						isIncoming,
					)
					if err != nil {
						log.Error().Err(err).Msg("Failed to upload sticker to S3")
					} else {
						postmap["s3"] = s3Data
					}
				}

				if s3Config.MediaDelivery == "base64" || s3Config.MediaDelivery == "both" {
					base64String, mimeType, err := fileToBase64(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to convert sticker to base64")
						return
					}
					postmap["base64"] = base64String
					postmap["mimeType"] = mimeType
					postmap["fileName"] = filepath.Base(tmpPath)
				}

				postmap["isSticker"] = true
				postmap["stickerAnimated"] = sticker.GetIsAnimated()

				if err := os.Remove(tmpPath); err != nil {
					log.Error().Err(err).Msg("Failed to delete temporary file")
				}
			}

		}

		var historyLimit int
		userinfo, found := userinfocache.Get(mycli.token)
		if found {
			historyStr := userinfo.(Values).Get("History")
			historyLimit, _ = strconv.Atoi(historyStr)
		} else {
			log.Warn().Str("userID", mycli.userID).Msg("User info not found in cache, skipping history")
			historyLimit = 0
		}

		if historyLimit > 0 {
			messageType := "text"
			textContent := ""
			mediaLink := ""
			caption := ""
			replyToMessageID := ""

			if protocolMsg := evt.Message.GetProtocolMessage(); protocolMsg != nil && protocolMsg.GetType() == 0 {
				messageType = "delete"
				if protocolMsg.GetKey() != nil {
					textContent = protocolMsg.GetKey().GetID()
				}
				log.Info().Str("deletedMessageID", textContent).Str("messageID", evt.Info.ID).Msg("Delete message detected")
			} else if reaction := evt.Message.GetReactionMessage(); reaction != nil {
				messageType = "reaction"
				replyToMessageID = reaction.GetKey().GetID()
				textContent = reaction.GetText()
			} else if img := evt.Message.GetImageMessage(); img != nil {
				messageType = "image"
				caption = img.GetCaption()
			} else if video := evt.Message.GetVideoMessage(); video != nil {
				messageType = "video"
				caption = video.GetCaption()
			} else if audio := evt.Message.GetAudioMessage(); audio != nil {
				messageType = "audio"
			} else if doc := evt.Message.GetDocumentMessage(); doc != nil {
				messageType = "document"
				caption = doc.GetCaption()
			} else if sticker := evt.Message.GetStickerMessage(); sticker != nil {
				messageType = "sticker"
			} else if contact := evt.Message.GetContactMessage(); contact != nil {
				messageType = "contact"
				textContent = contact.GetDisplayName()
			} else if location := evt.Message.GetLocationMessage(); location != nil {
				messageType = "location"
				textContent = location.GetName()
			}

			if messageType != "reaction" && messageType != "delete" {
				if conv := evt.Message.GetConversation(); conv != "" {
					textContent = conv
				} else if ext := evt.Message.GetExtendedTextMessage(); ext != nil {
					textContent = ext.GetText()
					if contextInfo := ext.GetContextInfo(); contextInfo != nil && contextInfo.GetStanzaId() != "" {
						replyToMessageID = contextInfo.GetStanzaId()
					}
				} else {
					textContent = caption
				}

				if textContent == "" {
					switch messageType {
					case "image":
						textContent = ":image:"
					case "video":
						textContent = ":video:"
					case "audio":
						textContent = ":audio:"
					case "document":
						textContent = ":document:"
					case "sticker":
						textContent = ":sticker:"
					case "contact":
						if textContent == "" {
							textContent = ":contact:"
						}
					case "location":
						if textContent == "" {
							textContent = ":location:"
						}
					}
				}
			}

			if s3Data, ok := postmap["s3"].(map[string]interface{}); ok {
				if url, ok := s3Data["url"].(string); ok {
					mediaLink = url
				}
			}

			if textContent != "" || mediaLink != "" || (messageType != "text" && messageType != "reaction") || messageType == "delete" {
				_, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
				evtJSON, err := json.Marshal(evt)
				if err != nil {
					log.Error().Err(err).Msg("Failed to marshal event to JSON")
					evtJSON = []byte("{}")
				}

				err = mycli.s.saveMessageToHistory(
					mycli.userID,
					realChatJID.User,
					realSenderJID.User,
					evt.Info.ID,
					messageType,
					textContent,
					mediaLink,
					replyToMessageID,
					string(evtJSON),
				)
				if err != nil {
					log.Error().Err(err).Msg("Failed to save message to history")
				} else {
					err = mycli.s.trimMessageHistory(mycli.userID, realChatJID.User, historyLimit)
					if err != nil {
						log.Error().Err(err).Msg("Failed to trim message history")
					}
				}
			} else {
				log.Debug().Str("messageType", messageType).Str("messageID", evt.Info.ID).Msg("Skipping empty message from history")
			}
		}

	case *events.Receipt:
		postmap["type"] = "ReadReceipt"
		dowebhook = 1
		
		// CORREÇÃO: Resolver LID para JID em receipts
		realChatJID := evt.Chat
		if isLID(evt.Chat) {
			log.Debug().Str("userID", mycli.userID).Str("lid", evt.Chat.User).Msg("LID detected in receipt, resolving...")
			realChatJID = resolveRealJID(mycli, evt.Chat, types.EmptyJID)
		}
		
		// Adicionar JID real ao postmap
		postmap["chatJID"] = realChatJID.String()
		postmap["chatJIDUser"] = realChatJID.User
		
		if evt.Type == types.ReceiptTypeRead || evt.Type == types.ReceiptTypeReadSelf {
			log.Info().Strs("id", evt.MessageIDs).Str("source", evt.SourceString()).Str("timestamp", fmt.Sprintf("%v", evt.Timestamp)).Msg("Message was read")
			if evt.Type == types.ReceiptTypeRead {
				postmap["state"] = "Read"
			} else {
				postmap["state"] = "ReadSelf"
			}
		} else if evt.Type == types.ReceiptTypeDelivered {
			postmap["state"] = "Delivered"
			log.Info().Str("id", evt.MessageIDs[0]).Str("source", evt.SourceString()).Str("timestamp", fmt.Sprintf("%v", evt.Timestamp)).Msg("Message delivered")
		} else {
			return
		}
	case *events.Presence:
		postmap["type"] = "Presence"
		dowebhook = 1
		
		// CORREÇÃO: Resolver LID para JID em presence
		realFromJID := evt.From
		if isLID(evt.From) {
			log.Debug().Str("userID", mycli.userID).Str("lid", evt.From.User).Msg("LID detected in presence, resolving...")
			realFromJID = resolveRealJID(mycli, evt.From, types.EmptyJID)
		}
		
		// Adicionar JID real ao postmap
		postmap["fromJID"] = realFromJID.String()
		postmap["fromJIDUser"] = realFromJID.User
		
		if evt.Unavailable {
			postmap["state"] = "offline"
			if evt.LastSeen.IsZero() {
				log.Info().Str("from", realFromJID.String()).Msg("User is now offline")
			} else {
				log.Info().Str("from", realFromJID.String()).Str("lastSeen", fmt.Sprintf("%v", evt.LastSeen)).Msg("User is now offline")
			}
		} else {
			postmap["state"] = "online"
			log.Info().Str("from", realFromJID.String()).Msg("User is now online")
		}
	case *events.HistorySync:
		postmap["type"] = "HistorySync"
		dowebhook = 1
	case *events.AppState:
		log.Info().Str("index", fmt.Sprintf("%+v", evt.Index)).Str("actionValue", fmt.Sprintf("%+v", evt.SyncActionValue)).Msg("App state event received")
	case *events.LoggedOut:
		postmap["type"] = "Logged Out"
		dowebhook = 1
		log.Info().Str("reason", evt.Reason.String()).Msg("Logged out")
		defer func() {
			select {
			case killchannel[mycli.userID] <- true:
			default:
			}
		}()
		sqlStmt := `UPDATE users SET connected=0 WHERE id=$1`
		_, err := mycli.db.Exec(sqlStmt, mycli.userID)
		if err != nil {
			log.Error().Err(err).Msg(sqlStmt)
			return
		}
	case *events.ChatPresence:
		postmap["type"] = "ChatPresence"
		dowebhook = 1
		
		// CORREÇÃO: Resolver LID para JID em chat presence
		isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.MessageSource.Chat, evt.MessageSource.Sender, types.EmptyJID)
		
		// Adicionar JIDs reais ao postmap
		postmap["chatJID"] = realChatJID.String()
		postmap["senderJID"] = realSenderJID.String()
		postmap["chatJIDUser"] = realChatJID.User
		postmap["senderJIDUser"] = realSenderJID.User
		postmap["isGroup"] = isGroup
		
		log.Info().Str("state", fmt.Sprintf("%s", evt.State)).Str("media", fmt.Sprintf("%s", evt.Media)).Str("chat", realChatJID.String()).Str("sender", realSenderJID.String()).Msg("Chat Presence received")
	case *events.CallOffer:
		postmap["type"] = "CallOffer"
		dowebhook = 1
		log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call offer")
	case *events.CallAccept:
		postmap["type"] = "CallAccept"
		dowebhook = 1
		log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call accept")
	case *events.CallTerminate:
		postmap["type"] = "CallTerminate"
		dowebhook = 1
		log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call terminate")
	case *events.CallOfferNotice:
		postmap["type"] = "CallOfferNotice"
		dowebhook = 1
		log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call offer notice")
	case *events.CallRelayLatency:
		postmap["type"] = "CallRelayLatency"
		dowebhook = 1
		log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call relay latency")
	case *events.Disconnected:
		postmap["type"] = "Disconnected"
		dowebhook = 1
		log.Info().Str("reason", fmt.Sprintf("%+v", evt)).Msg("Disconnected from Whatsapp")
	case *events.ConnectFailure:
		postmap["type"] = "ConnectFailure"
		dowebhook = 1
		log.Error().Str("reason", fmt.Sprintf("%+v", evt)).Msg("Failed to connect to Whatsapp")
	case *events.UndecryptableMessage:
		postmap["type"] = "UndecryptableMessage"
		dowebhook = 1
		log.Warn().Str("info", evt.Info.SourceString()).Msg("Undecryptable message received")
	case *events.MediaRetry:
		postmap["type"] = "MediaRetry"
		dowebhook = 1
		log.Info().Str("messageID", evt.MessageID).Msg("Media retry event")
	case *events.GroupInfo:
		postmap["type"] = "GroupInfo"
		dowebhook = 1
		log.Info().Str("jid", evt.JID.String()).Msg("Group info updated")
	case *events.JoinedGroup:
		postmap["type"] = "JoinedGroup"
		dowebhook = 1
		log.Info().Str("jid", evt.JID.String()).Msg("Joined group")
	case *events.Picture:
		postmap["type"] = "Picture"
		dowebhook = 1
		log.Info().Str("jid", evt.JID.String()).Msg("Picture updated")
	case *events.BlocklistChange:
		postmap["type"] = "BlocklistChange"
		dowebhook = 1
		log.Info().Str("jid", evt.JID.String()).Msg("Blocklist changed")
	case *events.Blocklist:
		postmap["type"] = "Blocklist"
		dowebhook = 1
		log.Info().Msg("Blocklist received")
	case *events.KeepAliveRestored:
		postmap["type"] = "KeepAliveRestored"
		dowebhook = 1
		log.Info().Msg("Keep alive restored")
	case *events.KeepAliveTimeout:
		postmap["type"] = "KeepAliveTimeout"
		dowebhook = 1
		log.Warn().Msg("Keep alive timeout")
	case *events.ClientOutdated:
		postmap["type"] = "ClientOutdated"
		dowebhook = 1
		log.Warn().Msg("Client outdated")
	case *events.TemporaryBan:
		postmap["type"] = "TemporaryBan"
		dowebhook = 1
		log.Info().Msg("Temporary ban")
	case *events.StreamError:
		postmap["type"] = "StreamError"
		dowebhook = 1
		log.Error().Str("code", evt.Code).Msg("Stream error")
	case *events.PairError:
		postmap["type"] = "PairError"
		dowebhook = 1
		log.Error().Msg("Pair error")
	case *events.PrivacySettings:
		postmap["type"] = "PrivacySettings"
		dowebhook = 1
		log.Info().Msg("Privacy settings updated")
	case *events.UserAbout:
		postmap["type"] = "UserAbout"
		dowebhook = 1
		log.Info().Str("jid", evt.JID.String()).Msg("User about updated")
	case *events.OfflineSyncCompleted:
		postmap["type"] = "OfflineSyncCompleted"
		dowebhook = 1
		log.Info().Msg("Offline sync completed")
	case *events.OfflineSyncPreview:
		postmap["type"] = "OfflineSyncPreview"
		dowebhook = 1
		log.Info().Msg("Offline sync preview")
	case *events.IdentityChange:
		postmap["type"] = "IdentityChange"
		dowebhook = 1
		log.Info().Str("jid", evt.JID.String()).Msg("Identity changed")
	case *events.NewsletterJoin:
		postmap["type"] = "NewsletterJoin"
		dowebhook = 1
		log.Info().Str("jid", evt.ID.String()).Msg("Newsletter joined")
	case *events.NewsletterLeave:
		postmap["type"] = "NewsletterLeave"
		dowebhook = 1
		log.Info().Str("jid", evt.ID.String()).Msg("Newsletter left")
	case *events.NewsletterMuteChange:
		postmap["type"] = "NewsletterMuteChange"
		dowebhook = 1
		log.Info().Str("jid", evt.ID.String()).Msg("Newsletter mute changed")
	case *events.NewsletterLiveUpdate:
		postmap["type"] = "NewsletterLiveUpdate"
		dowebhook = 1
		log.Info().Msg("Newsletter live update")
	case *events.FBMessage:
		postmap["type"] = "FBMessage"
		dowebhook = 1
		
		// CORREÇÃO: Resolver LID para JID nas mensagens do Facebook/Instagram
		isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.SenderAlt)
		
		// Salvar o mapeamento LID se disponível
		if isLID(evt.Info.Sender) && evt.Info.SenderAlt.Server != "" && !isLID(evt.Info.SenderAlt) {
			saveLIDForUser(mycli, evt.Info.SenderAlt.User, evt.Info.Sender.User)
			log.Info().
				Str("lid", evt.Info.Sender.User).
				Str("real_jid", evt.Info.SenderAlt.User).
				Msg("✅ LID do Facebook/Instagram mapeado")
		}
		
		// Usar o JID real no postmap
		if isGroup {
			postmap["chatJID"] = realChatJID.String()
			postmap["senderJID"] = realSenderJID.String()
			postmap["chatJIDUser"] = realChatJID.User
			postmap["senderJIDUser"] = realSenderJID.User
		} else {
			postmap["chatJID"] = realSenderJID.String()
			postmap["senderJID"] = realSenderJID.String()
			postmap["chatJIDUser"] = realSenderJID.User
			postmap["senderJIDUser"] = realSenderJID.User
		}
		
		log.Info().
			Str("info", evt.Info.SourceString()).
			Str("realChatJID", realChatJID.User).
			Str("realSenderJID", realSenderJID.User).
			Bool("isGroup", isGroup).
			Msg("📱 Mensagem do Facebook/Instagram recebida (LID resolvido)")
	default:
		log.Warn().Str("event", fmt.Sprintf("%+v", evt)).Msg("Unhandled event")
	}

	if dowebhook == 1 {
		sendEventWithWebHook(mycli, postmap, path)
	}
}
