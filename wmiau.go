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
	"os"
	"path/filepath"
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
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/encoding/protojson"
)

// ============================================================================
// LID/JID MANAGEMENT - Sistema completo com PostgreSQL e Cache
// ============================================================================

// Estrutura para mapear a tabela whatsmeow_lid_map
type LIDMapping struct {
	LID string `db:"lid"`
	PN  string `db:"pn"`
}

var jidLidCache = cache.New(24*time.Hour, 1*time.Hour)
var lidCacheMutex sync.RWMutex

// Connection webhook mapping
var connectionMutex sync.RWMutex
var connectionWebhooks = make(map[string]string)

func isLID(jid types.JID) bool {
	return jid.Server == types.HiddenUserServer
}

func getRealJID(primary types.JID, alt types.JID) (realJID types.JID, lidJID types.JID) {
	if isLID(primary) {
		return alt, primary
	}
	return primary, alt
}

// ⭐ NOVA FUNÇÃO: getRealJIDWithFallback - SEMPRE resolve o número real usando TODOS os métodos
func getRealJIDWithFallback(mycli *MyClient, primary types.JID, alt types.JID, chatJID types.JID) (realJID types.JID, lidJID types.JID) {
	if !isLID(primary) {
		// Primary não é LID - retorna normalmente
		return primary, alt
	}
	
	// Primary é LID - precisa resolver COM CERTEZA
	log.Info().
		Str("lid", primary.User).
		Str("alt", alt.String()).
		Str("chatJID", chatJID.String()).
		Msg("🔍 [MODO AGRESSIVO] Resolvendo LID com TODOS os métodos...")
	
	// Opção 1: Usar alt se disponível
	if !alt.IsEmpty() && !isLID(alt) {
		log.Info().Str("realJID", alt.User).Msg("  ✅ Método 1: Usando senderAlt")
		saveLIDForUser(mycli, alt.User, primary.User)
		return alt, primary
	}
	
	// Opção 2: Buscar no banco/cache
	if phone, found := resolveLIDForUser(mycli, primary.User); found {
		realJID := types.NewJID(phone, types.DefaultUserServer)
		log.Info().
			Str("lid", primary.User).
			Str("phone", phone).
			Msg("  ✅ Método 2: LID resolvido do banco/cache!")
		return realJID, primary
	}
	
	// Opção 3: Se for chat individual, usar o chatJID (MUITO COMUM EM ANÚNCIOS!)
	if chatJID.Server == types.DefaultUserServer && !isLID(chatJID) {
		log.Info().
			Str("lid", primary.User).
			Str("chatJID", chatJID.User).
			Msg("  ✅ Método 3: Usando chatJID como número real (ANÚNCIO)")
		saveLIDForUser(mycli, chatJID.User, primary.User)
		return chatJID, primary
	}
	
	// Opção 4: Se for grupo, FORÇAR buscar nos participantes
	if chatJID.Server == types.GroupServer {
		log.Info().
			Str("lid", primary.User).
			Str("groupJID", chatJID.String()).
			Msg("  🔍 Método 4: Forçando busca no grupo...")
		
		ctx4, cancel4 := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel4()
		
		groupInfo, err := mycli.WAClient.GetGroupInfo(ctx4, chatJID)
		if err == nil && groupInfo != nil {
			for _, participant := range groupInfo.Participants {
				if !participant.LID.IsEmpty() && participant.LID.User == primary.User {
					log.Info().
						Str("lid", primary.User).
						Str("phone", participant.JID.User).
						Msg("  ✅ Método 4: LID resolvido nos participantes!")
					saveLIDForUser(mycli, participant.JID.User, primary.User)
					return participant.JID, primary
				}
			}
		}
	}
	
	// Opção 5: FORÇAR SUBSCRIÇÃO AO PRESENCE para atualizar dados
	log.Warn().
		Str("lid", primary.User).
		Msg("  ⚠️ Método 5: Tentando forçar atualização via presence...")
	
	// Força refresh do contato
	if mycli.WAClient != nil {
		ctx5, cancel5 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel5()
		
		// Tenta subscrever ao presence para forçar WhatsApp enviar dados
		err := mycli.WAClient.SubscribePresence(ctx5, primary)
		if err == nil {
			log.Info().Str("lid", primary.User).Msg("  ✅ Subscrito ao presence - aguardando dados...")
			// Aguarda um pouco para ver se recebe atualização
			time.Sleep(2 * time.Second)
			
			// Tenta novamente no banco após a subscrição
			if phone, found := resolveLIDForUser(mycli, primary.User); found {
				realJID := types.NewJID(phone, types.DefaultUserServer)
				log.Info().
					Str("lid", primary.User).
					Str("phone", phone).
					Msg("  ✅ Método 5: LID resolvido após presence update!")
				return realJID, primary
			}
		}
	}
	
	// ÚLTIMO RECURSO: Se o chatJID for individual MAS tiver LID, usar mesmo assim
	if chatJID.Server == types.DefaultUserServer {
		log.Warn().
			Str("lid", primary.User).
			Str("chatJID", chatJID.User).
			Msg("  ⚠️ FALLBACK FINAL: Usando chatJID mesmo sendo LID possível")
		saveLIDForUser(mycli, chatJID.User, primary.User)
		return chatJID, primary
	}
	
	// CRÍTICO: NÃO conseguiu resolver de jeito nenhum
	log.Error().
		Str("lid", primary.User).
		Str("chatServer", chatJID.Server).
		Str("chatJID", chatJID.String()).
		Msg("  ❌❌❌ FALHA CRÍTICA: Não conseguiu resolver LID com NENHUM método!")
	
	// Retorna pelo menos o LID para não perder a mensagem
	return types.JID{}, primary
}

// Salva LID no banco de dados PostgreSQL
func saveLIDToDB(db *sqlx.DB, jid string, lid string, pn string) error {
	if db == nil {
		log.Error().Msg("❌ Database connection is nil")
		return errors.New("database connection is nil")
	}

	if lid == "" || pn == "" {
		log.Warn().Str("lid", lid).Str("pn", pn).Msg("❌ Valores vazios")
		return errors.New("valores vazios não permitidos")
	}

	query := `
		INSERT INTO public.whatsmeow_lid_map (lid, pn)
		VALUES ($1, $2)
		ON CONFLICT (lid) 
		DO UPDATE SET 
			pn = EXCLUDED.pn
	`

	_, err := db.Exec(query, lid, pn)
	if err != nil {
		log.Error().Err(err).Str("lid", lid).Str("pn", pn).Msg("❌ Erro ao salvar LID")
		return err
	}

	log.Debug().Str("lid", lid).Str("pn", pn).Msg("✅ LID salvo no banco")
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

	// 3. Salvar no whatsmeow Store.LIDs também (compatibilidade)
	if mycli.WAClient != nil && mycli.WAClient.Store != nil && mycli.WAClient.Store.LIDs != nil {
		lidJID := types.NewJID(lid, types.HiddenUserServer)
		pnJID := types.NewJID(phone, types.DefaultUserServer)
		err := mycli.WAClient.Store.LIDs.PutLIDMapping(context.Background(), lidJID, pnJID)
		if err != nil {
			log.Debug().Err(err).Msg("  ⚠️ Falha ao salvar no whatsmeow LID store")
		} else {
			log.Debug().Msg("  ✅ Salvo no whatsmeow LID store")
		}
	}

	log.Info().Str("userID", mycli.userID).Str("phone", phone).Str("lid", lid).Msg("✅ LID mapeado e persistido!")
}

// Resolve LID para número usando cache + PostgreSQL + whatsmeow
func resolveLIDForUser(mycli *MyClient, lid string) (string, bool) {
	if lid == "" {
		return "", false
	}

	log.Debug().Str("userID", mycli.userID).Str("lid", lid).Msg("🔍 Resolvendo LID...")

	// 1. Verificar cache em memória
	cacheKey := fmt.Sprintf("%s:%s", mycli.userID, lid)
	lidCacheMutex.RLock()
	if phone, found := jidLidCache.Get(cacheKey); found {
		lidCacheMutex.RUnlock()
		phoneStr := phone.(string)
		log.Debug().Str("lid", lid).Str("phone", phoneStr).Msg("  ✅ CACHE HIT")
		return phoneStr, true
	}
	lidCacheMutex.RUnlock()

	// 2. Verificar whatsmeow Store.LIDs
	if mycli.WAClient != nil && mycli.WAClient.Store != nil && mycli.WAClient.Store.LIDs != nil {
		lidJID := types.NewJID(lid, types.HiddenUserServer)
		pnJID, err := mycli.WAClient.Store.LIDs.GetPNForLID(context.Background(), lidJID)
		if err == nil && !pnJID.IsEmpty() {
			log.Debug().Str("lid", lid).Str("phone", pnJID.User).Msg("  ✅ WHATSMEOW STORE HIT")
			// Atualizar cache
			lidCacheMutex.Lock()
			jidLidCache.Set(cacheKey, pnJID.User, cache.DefaultExpiration)
			lidCacheMutex.Unlock()
			return pnJID.User, true
		}
	}

	// 3. Buscar no PostgreSQL
	log.Debug().Str("lid", lid).Msg("  🔎 Buscando no banco...")
	
	var mapping LIDMapping
	query := `SELECT lid, pn FROM public.whatsmeow_lid_map WHERE lid = $1 LIMIT 1`
	
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

	// 4. Atualizar cache
	lidCacheMutex.Lock()
	jidLidCache.Set(cacheKey, mapping.PN, cache.DefaultExpiration)
	lidCacheMutex.Unlock()
	log.Debug().Str("lid", lid).Msg("  💾 Cache atualizado")

	return mapping.PN, true
}

// Carrega todos LIDs do banco para o cache (chamar ao iniciar)
func loadLIDsForUser(mycli *MyClient) {
	log.Info().Str("userID", mycli.userID).Msg("📂 Carregando LIDs do banco...")

	var mappings []LIDMapping
	query := `SELECT lid, pn FROM public.whatsmeow_lid_map ORDER BY lid DESC`
	
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

// Identifica tipo de chat e resolve LIDs - FUNÇÃO PRINCIPAL ⭐
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
		// ⭐ USA O FALLBACK AGORA
		senderReal, senderLID := getRealJIDWithFallback(mycli, senderJID, senderAlt, chatJID)
		
		if isLID(senderLID) && !senderReal.IsEmpty() {
			log.Info().
				Str("senderReal", senderReal.User).
				Str("senderLID", senderLID.User).
				Msg("  🆔 LID detectado em GRUPO - salvando...")
			saveLIDForUser(mycli, senderReal.User, senderLID.User)
		} else if isLID(senderLID) && senderReal.IsEmpty() {
			log.Warn().
				Str("senderLID", senderLID.User).
				Msg("  ⚠️  LID não pode ser resolvido em GRUPO")
		} else {
			log.Debug().Str("senderReal", senderReal.User).Msg("  ✓ Sender não é LID")
		}
		
		return true, realChatJID, senderReal
	} else {
		log.Debug().Str("chatJID", chatJID.String()).Msg("  👤 CHAT INDIVIDUAL detectado")
		realChatJID := resolveRealJID(mycli, chatJID, senderAlt)
		// ⭐ USA O FALLBACK AGORA - passa chatJID também
		senderReal, senderLID := getRealJIDWithFallback(mycli, senderJID, senderAlt, chatJID)
		
		if isLID(senderLID) && !senderReal.IsEmpty() {
			log.Info().
				Str("senderReal", senderReal.User).
				Str("senderLID", senderLID.User).
				Msg("  🆔 LID detectado em CHAT INDIVIDUAL - salvando...")
			saveLIDForUser(mycli, senderReal.User, senderLID.User)
		} else if isLID(senderLID) && senderReal.IsEmpty() {
			log.Warn().
				Str("senderLID", senderLID.User).
				Msg("  ⚠️  LID não pode ser resolvido em CHAT INDIVIDUAL")
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

// ⭐ NOVA FUNÇÃO: Resolve JID para número real (usa todo o sistema de LID)
func resolveJIDToRealNumber(mycli *MyClient, jid types.JID) string {
	if jid.IsEmpty() {
		return ""
	}

	// Se é LID, tenta resolver
	if isLID(jid) {
		if phone, found := resolveLIDForUser(mycli, jid.User); found {
			// Retorna o número completo formatado
			resolvedJID := fmt.Sprintf("%s@%s", phone, types.DefaultUserServer)
			log.Debug().
				Str("jid", jid.String()).
				Str("resolvedJID", resolvedJID).
				Msg("✅ JID (LID) resolvido para número real")
			return resolvedJID
		}
		log.Warn().
			Str("jid", jid.String()).
			Msg("⚠️  Não foi possível resolver LID, retornando LID original")
		// Retorna o LID original completo se não conseguiu resolver
		return jid.String()
	}

	// Se é número normal, retorna o JID completo
	if jid.Server == types.DefaultUserServer {
		return jid.String()
	}

	// Para grupos, retorna o JID completo
	if jid.Server == types.GroupServer {
		return jid.String()
	}

	// Fallback: retorna o JID completo
	return jid.String()
}

// ============================================================================
// FIM DO SISTEMA DE LID
// ============================================================================


// downloadWhatsAppMediaWithRetry baixa mídia do WhatsApp com sistema de retry
func downloadWhatsAppMediaWithRetry(client *whatsmeow.Client, mediaType string, media interface{}, maxRetries int) ([]byte, error) {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Debug().
			Str("mediaType", mediaType).
			Int("attempt", attempt).
			Int("maxRetries", maxRetries).
			Msg("Tentativa de download de mídia recebida do WhatsApp")

		var data []byte
		var err error

		// Fazer download baseado no tipo de mídia
		switch m := media.(type) {
		case *waE2E.ImageMessage:
			data, err = client.Download(context.Background(), m)
		case *waE2E.VideoMessage:
			data, err = client.Download(context.Background(), m)
		case *waE2E.DocumentMessage:
			data, err = client.Download(context.Background(), m)
		case *waE2E.AudioMessage:
			data, err = client.Download(context.Background(), m)
		case *waE2E.StickerMessage:
			data, err = client.Download(context.Background(), m)
		default:
			return nil, fmt.Errorf("tipo de mídia não suportado: %T", media)
		}

		if err != nil {
			lastErr = fmt.Errorf("erro ao fazer download da mídia %s (tentativa %d/%d): %v", mediaType, attempt, maxRetries, err)
			if attempt < maxRetries {
				// Backoff exponencial: 1s, 2s, 4s
				backoff := time.Duration(attempt) * time.Second
				log.Warn().
					Err(err).
					Str("mediaType", mediaType).
					Dur("backoff", backoff).
					Int("attempt", attempt).
					Msg("Falha no download de mídia recebida, aguardando antes de tentar novamente")
				time.Sleep(backoff)
			}
			continue
		}

		// Se chegou até aqui, download foi bem-sucedido
		if len(data) > 0 {
			log.Info().
				Str("mediaType", mediaType).
				Int("attempt", attempt).
				Int("dataSize", len(data)).
				Msg("Download de mídia recebida bem-sucedido")
			return data, nil
		}

		// Se não há dados, tentar novamente
		lastErr = fmt.Errorf("download retornou dados vazios (tentativa %d/%d)", attempt, maxRetries)
		if attempt < maxRetries {
			backoff := time.Duration(attempt) * time.Second
			log.Warn().
				Str("mediaType", mediaType).
				Dur("backoff", backoff).
				Int("attempt", attempt).
				Msg("Download retornou dados vazios, aguardando antes de tentar novamente")
			time.Sleep(backoff)
		}
	}

	return nil, fmt.Errorf("falha no download de mídia %s após %d tentativas: %v", mediaType, maxRetries, lastErr)
}

// saveMediaWithRetry salva mídia com sistema de retry para operações de arquivo
func saveMediaWithRetry(data []byte, filePath string, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Debug().
			Str("filePath", filePath).
			Int("attempt", attempt).
			Int("maxRetries", maxRetries).
			Msg("Tentativa de salvar arquivo de mídia")

		// Tentar criar diretório pai se não existir
		dir := filepath.Dir(filePath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			lastErr = fmt.Errorf("erro ao criar diretório %s (tentativa %d/%d): %v", dir, attempt, maxRetries, err)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * time.Second
				log.Warn().
					Err(err).
					Str("dir", dir).
					Dur("backoff", backoff).
					Int("attempt", attempt).
					Msg("Falha ao criar diretório, aguardando antes de tentar novamente")
				time.Sleep(backoff)
			}
			continue
		}

		// Tentar salvar arquivo
		if err := os.WriteFile(filePath, data, os.ModePerm); err != nil {
			lastErr = fmt.Errorf("erro ao salvar arquivo %s (tentativa %d/%d): %v", filePath, attempt, maxRetries, err)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * time.Second
				log.Warn().
					Err(err).
					Str("filePath", filePath).
					Dur("backoff", backoff).
					Int("attempt", attempt).
					Msg("Falha ao salvar arquivo, aguardando antes de tentar novamente")
				time.Sleep(backoff)
			}
			continue
		}

		// Verificar se arquivo foi salvo corretamente
		if _, err := os.Stat(filePath); err != nil {
			lastErr = fmt.Errorf("arquivo não encontrado após salvamento %s (tentativa %d/%d): %v", filePath, attempt, maxRetries, err)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * time.Second
				log.Warn().
					Err(err).
					Str("filePath", filePath).
					Dur("backoff", backoff).
					Int("attempt", attempt).
					Msg("Arquivo não encontrado após salvamento, aguardando antes de tentar novamente")
				time.Sleep(backoff)
			}
			continue
		}

		// Sucesso!
		log.Info().
			Str("filePath", filePath).
			Int("attempt", attempt).
			Int("fileSize", len(data)).
			Msg("Arquivo de mídia salvo com sucesso")
		return nil
	}

	return fmt.Errorf("falha ao salvar arquivo %s após %d tentativas: %v", filePath, maxRetries, lastErr)
}

// createDirectoryWithRetry cria diretório com sistema de retry
func createDirectoryWithRetry(dirPath string, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Debug().
			Str("dirPath", dirPath).
			Int("attempt", attempt).
			Int("maxRetries", maxRetries).
			Msg("Tentativa de criar diretório")

		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			lastErr = fmt.Errorf("erro ao criar diretório %s (tentativa %d/%d): %v", dirPath, attempt, maxRetries, err)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * time.Second
				log.Warn().
					Err(err).
					Str("dirPath", dirPath).
					Dur("backoff", backoff).
					Int("attempt", attempt).
					Msg("Falha ao criar diretório, aguardando antes de tentar novamente")
				time.Sleep(backoff)
			}
			continue
		}

		// Verificar se diretório foi criado
		if _, err := os.Stat(dirPath); err != nil {
			lastErr = fmt.Errorf("diretório não encontrado após criação %s (tentativa %d/%d): %v", dirPath, attempt, maxRetries, err)
			if attempt < maxRetries {
				backoff := time.Duration(attempt) * time.Second
				log.Warn().
					Err(err).
					Str("dirPath", dirPath).
					Dur("backoff", backoff).
					Int("attempt", attempt).
					Msg("Diretório não encontrado após criação, aguardando antes de tentar novamente")
				time.Sleep(backoff)
			}
			continue
		}

		// Sucesso!
		log.Info().
			Str("dirPath", dirPath).
			Int("attempt", attempt).
			Msg("Diretório criado com sucesso")
		return nil
	}

	return fmt.Errorf("falha ao criar diretório %s após %d tentativas: %v", dirPath, maxRetries, lastErr)
}

// db field declaration as *sqlx.DB
type MyClient struct {
	WAClient       *whatsmeow.Client
	eventHandlerID uint32
	userID         string
	token          string
	subscriptions  []string
	db             *sqlx.DB
	connectionKey  string
}

func updateAndGetUserSubscriptions(mycli *MyClient) ([]string, error) {
	// Get updated events from cache/database
	currentEvents := ""
	userinfo2, found2, err := GetRedisCache().GetUserInfo(mycli.token)
	if found2 && err == nil {
		currentEvents = userinfo2.Get("Events")
	} else {
		// If not in cache, get from database
		if err := mycli.db.Get(&currentEvents, "SELECT events FROM users WHERE id=$1", mycli.userID); err != nil {
			log.Warn().Err(err).Str("userID", mycli.userID).Msg("Could not get events from DB")
			return nil, err // Propagate the error
		}
	}

	// Update client subscriptions if changed
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

	// Update the client subscriptions
	mycli.subscriptions = subscribedEvents

	return subscribedEvents, nil
}

func getUserWebhookUrl(token string) string {
	webhookurl := ""
	myuserinfo, found, err := GetRedisCache().GetUserInfo(token)
	if !found || err != nil {
		log.Warn().Str("token", token).Msg("Could not call webhook as there is no user for this token")
	} else {
		webhookurl = myuserinfo.Get("Webhook")
	}
	return webhookurl
}

func getConnectionWebhookUrl(connectionKey string) string {
	connectionMutex.RLock()
	webhook, exists := connectionWebhooks[connectionKey]
	connectionMutex.RUnlock()
	if !exists {
		log.Warn().Str("connectionKey", connectionKey).Msg("Connection webhook not found")
		return ""
	}
	return webhook
}

// GetCachedProfilePicture retrieves the profile picture for a given JID
func GetCachedProfilePicture(client *whatsmeow.Client, jid types.JID, preview bool) (*types.ProfilePictureInfo, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	pic, err := client.GetProfilePictureInfo(ctx, jid, &whatsmeow.GetProfilePictureParams{
		Preview: preview,
	})
	if err != nil {
		return nil, err
	}
	
	return pic, nil
}

// RedisCache stub - implementação mínima para compatibilidade
type RedisCache struct{}

func (rc *RedisCache) GetUserInfo(token string) (Values, bool, error) {
	// Fallback para cache em memória
	myuserinfo, found := userinfocache.Get(token)
	if found {
		return myuserinfo.(Values), true, nil
	}
	return Values{}, false, errors.New("user not found in cache")
}

func (rc *RedisCache) SetUserInfo(token string, v Values, duration time.Duration) {
	// Fallback para cache em memória
	userinfocache.Set(token, v, duration)
}

func (rc *RedisCache) SetLastMessage(userID string, info interface{}, duration time.Duration) {
	// Stub - não implementado
	log.Debug().Str("userID", userID).Msg("SetLastMessage called (stub)")
}

func GetRedisCache() *RedisCache {
	return &RedisCache{}
}

// sendToGlobalNATS stub - para compatibilidade
func sendToGlobalNATS(data []byte) {
	// Stub - não implementado
	log.Debug().Msg("sendToGlobalNATS called (stub)")
}

// sendCallEventToWebhook envia eventos de chamada diretamente para webhook HTTP
func sendCallEventToWebhook(mycli *MyClient, jsonData []byte) {
	webhookUrl := getUserWebhookUrl(mycli.token)
	if webhookUrl == "" {
		log.Warn().Str("userID", mycli.userID).Msg("No webhook URL found for call event")
		return
	}

	client := resty.New()
	client.SetTimeout(10 * time.Second)

	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(jsonData).
		Post(webhookUrl)

	if err != nil {
		log.Error().
			Err(err).
			Str("userID", mycli.userID).
			Str("webhookUrl", webhookUrl).
			Msg("Failed to send call event to webhook")
		return
	}

	if resp.StatusCode() >= 200 && resp.StatusCode() < 300 {
		log.Debug().
			Str("userID", mycli.userID).
			Int("statusCode", resp.StatusCode()).
			Msg("Call event sent to webhook successfully")
	} else {
		log.Warn().
			Str("userID", mycli.userID).
			Int("statusCode", resp.StatusCode()).
			Str("response", string(resp.Body())).
			Msg("Webhook returned non-success status for call event")
	}
}

func sendEventWithWebHook(mycli *MyClient, postmap map[string]interface{}, path string) {

	eventType, ok := postmap["type"].(string)
	if !ok {
		log.Warn().Msg("Event type not found in postmap")
		return
	}

	if !checkIfGlobalEventAllowed(eventType) {
		log.Debug().
			Str("type", eventType).
			Str("userID", mycli.userID).
			Msg("Event filtered out by global events configuration")
		return
	}

	postmap["token"] = mycli.token

	jsonData, err := json.Marshal(postmap)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal postmap to JSON")
		return
	}

	go sendToGlobalNATS(jsonData) // Se for evento de chamada, enviar diretamente para webhook HTTP

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

// Variáveis globais para configuração de eventos
var (
	globalEvents    []string
	globalEventsMap map[string]bool
)

// checkIfGlobalEventAllowed checks if an event type is allowed by global events configuration
func checkIfGlobalEventAllowed(eventType string) bool {
	// If no global events configured, allow all (default behavior)
	if len(globalEvents) == 0 {
		return true
	}

	// Check if "All" is configured
	if globalEventsMap["All"] {
		return true
	}

	// Check if specific event type is allowed
	return globalEventsMap[eventType]
}

func (s *server) connectOnStartup() {
	rows, err := s.db.Queryx("SELECT id,name,token,jid,webhook,events,proxy_url,CASE WHEN s3_enabled THEN 'true' ELSE 'false' END AS s3_enabled,media_delivery,connection_key FROM users WHERE connected=1")
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
		connectionKey := ""
		err = rows.Scan(&txtid, &name, &token, &jid, &webhook, &events, &proxy_url, &s3_enabled, &media_delivery, &connectionKey)
		if err != nil {
			log.Error().Err(err).Msg("DB Problem")
			return
		} else {
			log.Info().Str("token", token).Msg("Connect to Whatsapp on startup")
			v := Values{map[string]string{
				"Id":            txtid,
				"Name":          name,
				"Jid":           jid,
				"Webhook":       webhook,
				"Token":         token,
				"Proxy":         proxy_url,
				"Events":        events,
				"S3Enabled":     s3_enabled,
				"MediaDelivery": media_delivery,
			}}
			GetRedisCache().SetUserInfo(token, v, 5*time.Minute)

			// Store connection webhook in map if connectionKey exists
			if connectionKey != "" && webhook != "" {
				connectionMutex.Lock()
				connectionWebhooks[connectionKey] = webhook
				connectionMutex.Unlock()
				log.Info().Str("key", connectionKey).Str("webhook", webhook).Msg("Loaded connection webhook on startup")
			}

			// Gets and set subscription to webhook events
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
			go s.startClient(txtid, jid, token, subscribedEvents, connectionKey)

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

func (s *server) startClient(userID string, textjid string, token string, subscriptions []string, connectionKey string) {
	log.Info().Str("userid", userID).Str("jid", textjid).Msg("Starting websocket connection to Whatsapp")

	var deviceStore *store.Device
	var err error

	// First handle the device store initialization
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

	// Create the client with initialized deviceStore
	var client *whatsmeow.Client
	if *waDebug != "" {
		client = whatsmeow.NewClient(deviceStore, clientLog)
	} else {
		client = whatsmeow.NewClient(deviceStore, nil)
	}

	// Now we can use the client with the manager
	// Log de fallback de hosted/coexistência
	if hv := os.Getenv("WHATS_HOSTED_FALLBACK"); hv != "" {
		log.Warn().Str("WHATS_HOSTED_FALLBACK", hv).Msg("Hosted fallback ativado para pareamento/coexistência")
	}

	clientManager.SetWhatsmeowClient(userID, client)
	if textjid != "" {
		jid, _ := parseJID(textjid)
		deviceStore, err = container.GetDevice(context.Background(), jid)
		if err != nil {
			panic(err)
		}
	} else {
		log.Warn().Msg("No jid found. Creating new device")
		deviceStore = container.NewDevice()
	}

	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()
	store.DeviceProps.Os = osName

	clientManager.SetWhatsmeowClient(userID, client)
	mycli := MyClient{client, 1, userID, token, subscriptions, s.db, connectionKey}
	mycli.eventHandlerID = mycli.WAClient.AddEventHandler(mycli.myEventHandler)

	clientManager.SetMyClient(userID, &mycli)

	httpClient := resty.New()
	httpClient.SetRedirectPolicy(resty.FlexibleRedirectPolicy(15))
	if *waDebug == "DEBUG" {
		httpClient.SetDebug(true)
	}
	httpClient.SetTimeout(10 * time.Second) // Reduzir timeout para webhooks mais rápidos
	httpClient.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
	httpClient.OnError(func(req *resty.Request, err error) {
		if v, ok := err.(*resty.ResponseError); ok {
			// v.Response contains the last response from the server
			// v.Err contains the original error
			log.Debug().Str("response", v.Response.String()).Msg("resty error")
			log.Error().Err(v.Err).Msg("resty error")
		}
	})

	var proxyURL string
	err = s.db.Get(&proxyURL, "SELECT proxy_url FROM users WHERE id=$1", userID)
	if err == nil && proxyURL != "" {
		httpClient.SetProxy(proxyURL)
	}
	clientManager.SetHTTPClient(userID, httpClient)

	if client.Store.ID == nil {
		// No ID stored, new login
		qrChan, err := client.GetQRChannel(context.Background())
		if err != nil {
			// This error means that we're already logged in, so ignore it.
			if !errors.Is(err, whatsmeow.ErrQRStoreContainsID) {
				log.Error().Err(err).Msg("Failed to get QR channel")
				return
			}
		} else {
			err = client.Connect() // Si no conectamos no se puede generar QR
			if err != nil {
				log.Error().Err(err).Msg("Failed to connect client")
				return
			}

			// CORREÇÃO: Processar QR code de forma assíncrona para não bloquear eventos
			go func() {
				myuserinfo, found, _ := GetRedisCache().GetUserInfo(token)

				for evt := range qrChan {
					if evt.Event == "code" {
						// Display QR code in terminal (useful for testing/developing)
						if *logType != "json" {
							qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
							fmt.Println("QR code:\n", evt.Code)
						}
						// Store encoded/embeded base64 QR on database for retrieval with the /qr endpoint
						image, _ := qrcode.Encode(evt.Code, qrcode.Medium, 256)
						base64qrcode := "data:image/png;base64," + base64.StdEncoding.EncodeToString(image)

						// CORREÇÃO: Operações de banco de dados assíncronas
						go func() {
							sqlStmt := `UPDATE users SET qrcode=$1 WHERE id=$2`
							_, err := s.db.Exec(sqlStmt, base64qrcode, userID)
							if err != nil {
								log.Error().Err(err).Msg(sqlStmt)
							} else {
								if found {
									v := updateUserInfo(myuserinfo, "Qrcode", base64qrcode)
									GetRedisCache().SetUserInfo(token, v.(Values), 5*time.Minute)
									log.Info().Str("qrcode", base64qrcode).Msg("update cache userinfo with qr code")
								}
							}
						}()

						//send QR code with webhook
						postmap := make(map[string]interface{})
						postmap["event"] = evt.Event
						postmap["qrCodeBase64"] = base64qrcode
						postmap["type"] = "QR"

						sendEventWithWebHook(&mycli, postmap, "")

					} else if evt.Event == "timeout" {
						// CORREÇÃO: Operações de banco de dados assíncronas
						go func() {
							// Clear QR code from DB on timeout
							sqlStmt := `UPDATE users SET qrcode='' WHERE id=$1`
							_, err := s.db.Exec(sqlStmt, userID)
							if err != nil {
								log.Error().Err(err).Msg(sqlStmt)
							} else {
								if found {
									v := updateUserInfo(myuserinfo, "Qrcode", "")
									GetRedisCache().SetUserInfo(token, v.(Values), 5*time.Minute)
								}
							}
						}()

						//send QR timeout event with webhook
						postmap := make(map[string]interface{})
						postmap["event"] = evt.Event
						postmap["type"] = "QR_TIMEOUT"
						postmap["message"] = "QR code has expired and needs to be regenerated"

						sendEventWithWebHook(&mycli, postmap, "")

						log.Warn().Msg("QR timeout killing channel")
						clientManager.DeleteWhatsmeowClient(userID)
						clientManager.DeleteMyClient(userID)
						clientManager.DeleteHTTPClient(userID)
						killchannel[userID] <- true
					} else if evt.Event == "success" {
						log.Info().Msg("QR pairing ok!")
						// CORREÇÃO: Operações de banco de dados assíncronas
						go func() {
							// Clear QR code after pairing
							sqlStmt := `UPDATE users SET qrcode='', connected=1 WHERE id=$1`
							_, err := s.db.Exec(sqlStmt, userID)
							if err != nil {
								log.Error().Err(err).Msg(sqlStmt)
							} else {
								if found {
									v := updateUserInfo(myuserinfo, "Qrcode", "")
									GetRedisCache().SetUserInfo(token, v.(Values), 5*time.Minute)
								}
							}
						}()
					} else {
						log.Info().Str("event", evt.Event).Msg("Login event")
					}
				}
			}()
		}

	} else {
		// Already logged in, just connect
		log.Info().Msg("Already logged in, just connect")
		err = client.Connect()
		if err != nil {
			panic(err)
		}
	}

	// Keep connected client live until disconnected/killed
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
			//log.Info().Str("jid",textjid).Msg("Loop the loop")
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

// FUNÇÃO DESATIVADA - Processamento de mídia do histórico completamente desabilitado
func processHistoryMediaToBase64(mycli *MyClient, mediaMsg interface{}, mediaType string) map[string]interface{} {
	log.Debug().
		Str("mediaType", mediaType).
		Msg("History media processing disabled - skipping media download to avoid 403 errors")

	// Não processa mídia do histórico - função completamente desativada
	return nil
}

// FUNÇÃO DESATIVADA - Histórico de mensagens completamente desabilitado
func processHistoryInBatches(connectionId string, conversations []*waHistorySync.Conversation, client *whatsmeow.Client) {
	log.Debug().
		Str("connectionId", connectionId).
		Int("conversations", len(conversations)).
		Msg("History processing disabled - skipping all history batch processing")

	// Não processa nada - função completamente desativada
	return
}

// FUNÇÃO DESATIVADA - Webhook de conclusão do histórico completamente desabilitado
func sendHistoryCompletionWebhook(connectionId string, totalConversations int) {
	log.Debug().
		Str("connectionId", connectionId).
		Int("totalConversations", totalConversations).
		Msg("History completion webhook disabled - skipping webhook notification")

	// Não envia webhook de conclusão - função completamente desativada
	return
}

func (mycli *MyClient) myEventHandler(rawEvt interface{}) {
	// CORREÇÃO: Processar cada evento em goroutine separada para não bloquear outros eventos
	go func() {
		txtid := mycli.userID
		postmap := make(map[string]interface{})
		// ⚠️ NÃO adicionar rawEvt aqui - ele contém JIDs com @lid
		// postmap["event"] = rawEvt
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
			if mycli.WAClient.Store.ID != nil {
				postmap["phone"] = mycli.WAClient.Store.ID.User
				postmap["jid"] = mycli.WAClient.Store.ID.String()
			}
			dowebhook = 1
			if len(mycli.WAClient.Store.PushName) == 0 {
				break
			}
			// Send presence available when connecting and when the pushname is changed.
			// This makes sure that outgoing messages always have the right pushname.
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
			}
			
			// Carregar LIDs do banco para o cache
			go loadLIDsForUser(mycli)
		case *events.PairSuccess:
			log.Info().Str("userid", mycli.userID).Str("token", mycli.token).Str("ID", evt.ID.String()).Str("BusinessName", evt.BusinessName).Str("Platform", evt.Platform).Msg("QR Pair Success")
			jid := evt.ID
			sqlStmt := `UPDATE users SET jid=$1 WHERE id=$2`
			_, err := mycli.db.Exec(sqlStmt, jid, mycli.userID)
			if err != nil {
				log.Error().Err(err).Msg(sqlStmt)
			}
		case *events.PairError:
			postmap["type"] = "PairError"
			dowebhook = 1
			log.Error().
				Str("userid", mycli.userID).
				Str("error", fmt.Sprintf("%v", evt.Error)).
				Str("platform", evt.Platform).
				Str("business", evt.BusinessName).
				Msg("Pairing error")
			postmap["error"] = fmt.Sprintf("%v", evt.Error)
			postmap["platform"] = evt.Platform
			postmap["businessName"] = evt.BusinessName
			if hv := os.Getenv("WHATS_HOSTED_FALLBACK"); hv != "" {
				postmap["hostedFallback"] = hv
			}

		case *events.StreamReplaced:
			log.Info().Msg("Received StreamReplaced event")
			return
		case *events.Message:

			GetRedisCache().SetLastMessage(mycli.userID, &evt.Info, 24*time.Hour)

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
			if evt.IsViewOnce {
				metaParts = append(metaParts, "ephemeral")
			}

			log.Info().Str("id", evt.Info.ID).Str("source", evt.Info.SourceString()).Str("parts", strings.Join(metaParts, ", ")).Msg("Message Received")
			
			// ⭐ Log adicional com número real resolvido
			if evt.Info.MessageSource.IsFromMe {
				// Mensagem enviada - mostra para quem foi enviada
				realRecipientNumber := resolveJIDToRealNumber(mycli, evt.Info.Chat)
				log.Info().
					Str("id", evt.Info.ID).
					Str("recipientNumber", realRecipientNumber).
					Str("recipientJID", evt.Info.Chat.String()).
					Msg("✅ Message sent to recipient (IsFromMe)")
			} else {
				// Mensagem recebida - mostra de quem veio
				realSenderNumber := resolveJIDToRealNumber(mycli, evt.Info.Sender)
				log.Info().
					Str("id", evt.Info.ID).
					Str("senderNumber", realSenderNumber).
					Str("senderJID", evt.Info.Sender.String()).
					Msg("✅ Message received from sender")
			}

			// Logar payload completo da mensagem (JSON do protobuf)
			if evt.Message != nil {
				mo := protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true}
				if msgJSON, err := mo.Marshal(evt.Message); err == nil {
					// Payload bruto (pode ser grande). Mantemos em DEBUG.
					log.Debug().RawJSON("messagePayload", msgJSON).Str("id", evt.Info.ID).Msg("WhatsApp message payload (protobuf JSON)")
				}
			}

			// ⭐ USA IDENTIFYCHATTYPE - RESOLVE LID TUDO EM UMA LINHA! ⭐
			isGroup, realChatJID, realSenderJID := identifyChatType(mycli, evt.Info.Chat, evt.Info.Sender, evt.Info.MessageSource.SenderAlt)

			if isGroup {
				// GRUPO
				log.Debug().
					Str("chatJID", evt.Info.Chat.String()).
					Str("senderJID", evt.Info.Sender.String()).
					Str("senderAlt", evt.Info.MessageSource.SenderAlt.String()).
					Str("realSenderJID", realSenderJID.String()).
					Bool("realSenderEmpty", realSenderJID.IsEmpty()).
					Msg("📊 Processando mensagem de GRUPO")
				
				if realSenderJID.User != "" {
					postmap["senderNumber"] = realSenderJID.User
					
					log.Info().
						Str("messageID", evt.Info.ID).
						Str("senderNumber", realSenderJID.User).
						Str("groupID", realChatJID.String()).
						Msg("✅ Sender resolvido em mensagem de grupo")

					// Busca foto de perfil do sender
					pic, err := GetCachedProfilePicture(mycli.WAClient, realSenderJID, false)
					if err != nil {
						log.Debug().Err(err).Str("sender", realSenderJID.String()).Msg("Failed to get sender profile picture")
					} else if pic != nil {
						postmap["senderProfilePicture"] = map[string]interface{}{
							"id":  pic.ID,
							"url": pic.URL,
						}
					}
					postmap["senderName"] = evt.Info.PushName
				} else {
					// ⚠️ CRÍTICO: Sender não foi resolvido
					// Adiciona LID original como fallback e marca como não resolvido
					if !evt.Info.Sender.IsEmpty() {
						postmap["senderLID"] = evt.Info.Sender.User
						postmap["senderNumber"] = "" // Deixa explicitamente vazio
						postmap["lidUnresolved"] = true
						
						log.Warn().
							Str("messageID", evt.Info.ID).
							Str("senderLID", evt.Info.Sender.User).
							Str("groupID", realChatJID.String()).
							Msg("⚠️ LID não resolvido em grupo - enviando LID original no webhook")
					} else {
						// Caso extremamente raro - não tem nem LID
						log.Error().
							Str("messageID", evt.Info.ID).
							Str("groupID", realChatJID.String()).
							Msg("❌ ERRO CRÍTICO: Sender completamente desconhecido - ignorando mensagem")
						dowebhook = 0
						// Não processar essa mensagem
					}
				}

				// Get group info including group profile picture
				groupInfo, err := mycli.WAClient.GetGroupInfo(context.Background(), realChatJID)
				if err != nil {
					log.Debug().Err(err).Str("group", realChatJID.String()).Msg("Failed to get group info")
				} else if groupInfo != nil {
					groupData := map[string]interface{}{
						"name": groupInfo.Name,
						"jid":  realChatJID.String(),
					}

					// Get group profile picture
					groupPic, err := GetCachedProfilePicture(mycli.WAClient, realChatJID, false)
					if err != nil {
						log.Debug().Err(err).Str("group", realChatJID.String()).Msg("Failed to get group profile picture")
					} else if groupPic != nil {
						groupData["profilePicture"] = map[string]interface{}{
							"id":  groupPic.ID,
							"url": groupPic.URL,
						}
					}

					postmap["groupInfo"] = groupData
				}
			} else {
				// CHAT INDIVIDUAL
				var targetJID types.JID
				if evt.Info.MessageSource.IsFromMe {
					targetJID = realChatJID // Mensagem enviada - usa o chat (recipient)
				} else {
					targetJID = realSenderJID // Mensagem recebida - usa o sender
				}

				if targetJID.User != "" {
					// Busca foto de perfil
					pic, err := GetCachedProfilePicture(mycli.WAClient, targetJID, false)
					if err != nil {
						log.Debug().Err(err).Str("target", targetJID.String()).Msg("Failed to get profile picture")
					} else if pic != nil {
						// Adiciona foto de perfil com nome apropriado
						if evt.Info.MessageSource.IsFromMe {
							// Mensagem enviada - foto do recipient
							postmap["recipientProfilePicture"] = map[string]interface{}{
								"id":  pic.ID,
								"url": pic.URL,
							}
						} else {
							// Mensagem recebida - foto do sender
							postmap["senderProfilePicture"] = map[string]interface{}{
								"id":  pic.ID,
								"url": pic.URL,
							}
						}
					}

					if evt.Info.MessageSource.IsFromMe {
						// Mensagem enviada - resolve recipient
						postmap["recipientNumber"] = realChatJID.User

						// Busca info de contato
						contactInfo, err := mycli.WAClient.Store.Contacts.GetContact(context.Background(), realChatJID)
						if err != nil {
							log.Debug().Err(err).Str("recipientJID", realChatJID.String()).Msg("Failed to get contact info from store")
							postmap["recipientJID"] = realChatJID.String()
						} else if contactInfo.Found {
							if contactInfo.PushName != "" {
								postmap["recipientName"] = contactInfo.PushName
							}
							if contactInfo.BusinessName != "" {
								postmap["recipientBusinessName"] = contactInfo.BusinessName
							}
							postmap["recipientJID"] = realChatJID.String()
						} else {
							postmap["recipientJID"] = realChatJID.String()
						}
					} else {
						// Mensagem recebida - resolve sender
						postmap["senderNumber"] = realSenderJID.User
						postmap["senderName"] = evt.Info.PushName
						
						// Busca info de contato do sender
						contactInfo, err := mycli.WAClient.Store.Contacts.GetContact(context.Background(), realSenderJID)
						if err != nil {
							log.Debug().Err(err).Str("senderJID", realSenderJID.String()).Msg("Failed to get sender contact info from store")
						} else if contactInfo.Found {
							if contactInfo.BusinessName != "" {
								postmap["senderBusinessName"] = contactInfo.BusinessName
							}
							// Se tiver nome salvo nos contatos, usa ele
							if contactInfo.FullName != "" {
								postmap["contactName"] = contactInfo.FullName
							} else if contactInfo.PushName != "" && contactInfo.PushName != evt.Info.PushName {
								postmap["contactName"] = contactInfo.PushName
							}
						}
					}
				}
			}

			if !*skipMedia {
				// try to get Image if any
				img := evt.Message.GetImageMessage()
				if img != nil {
					// Create a temporary directory with retry
					tmpDirectory := filepath.Join(os.TempDir(), "user_"+txtid)
					errDir := createDirectoryWithRetry(tmpDirectory, 3)
					if errDir != nil {
						log.Error().Err(errDir).Msg("Could not create temporary directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Download the image with retry
					data, err := downloadWhatsAppMediaWithRetry(mycli.WAClient, "image", img, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to download image after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Determine the file extension based on the MIME type
					exts, _ := mime.ExtensionsByType(img.GetMimetype())
					tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+exts[0])

					// Write the image to the temporary file with retry
					err = saveMediaWithRetry(data, tmpPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save image to temporary file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Save media file and generate URL using API_BASE_URL with retry
					mediaDir := filepath.Join("medias", txtid)
					err = createDirectoryWithRetry(mediaDir, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to create media directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate unique filename
					fileName := fmt.Sprintf("%s_%s%s", evt.Info.ID, fmt.Sprintf("%d", time.Now().Unix()), exts[0])
					mediaPath := filepath.Join(mediaDir, fileName)

					// Copy file to media directory with retry
					err = saveMediaWithRetry(data, mediaPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save media file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate URL using API_BASE_URL
					apiBaseURL := os.Getenv("API_BASE_URL")
					if apiBaseURL == "" {
						apiBaseURL = "http://localhost:8080"
					}
					mediaURL := fmt.Sprintf("%s/medias/%s/%s", apiBaseURL, txtid, fileName)

					// Add media info to postmap
					postmap["mediaUrl"] = mediaURL
					postmap["mimeType"] = img.GetMimetype()
					postmap["fileName"] = fileName

					// Logar a URL gerada da mídia
					log.Info().Str("id", evt.Info.ID).Str("fileName", fileName).Str("mime", img.GetMimetype()).Str("mediaUrl", mediaURL).Msg("Generated mediaUrl for received image")

					// Convert the image to base64 if needed (mantido para compatibilidade)
					{
						_, mimeType, err := fileToBase64(tmpPath)
						if err != nil {
							log.Error().Err(err).Msg("Failed to convert image to base64")
							return
						}

						// Media saved to local directory and URL generated
						postmap["mimeType"] = mimeType
						postmap["fileName"] = filepath.Base(tmpPath)
					}

					// Log the successful conversion
					log.Info().Str("path", tmpPath).Msg("Image processed")

					// Delete the temporary file
					err = os.Remove(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to delete temporary file")
					} else {
						log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
					}
				}

				// try to get Audio if any
				audio := evt.Message.GetAudioMessage()
				if audio != nil {
					// Create a temporary directory with retry
					tmpDirectory := filepath.Join(os.TempDir(), "user_"+txtid)
					errDir := createDirectoryWithRetry(tmpDirectory, 3)
					if errDir != nil {
						log.Error().Err(errDir).Msg("Could not create temporary directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Download the audio with retry
					data, err := downloadWhatsAppMediaWithRetry(mycli.WAClient, "audio", audio, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to download audio after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Determine the file extension based on the MIME type
					exts, _ := mime.ExtensionsByType(audio.GetMimetype())
					var ext string
					if len(exts) > 0 {
						ext = exts[0]
					} else {
						ext = ".ogg" // Default extension if MIME type is not recognized
					}
					tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+ext)

					// Write the audio to the temporary file with retry
					err = saveMediaWithRetry(data, tmpPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save audio to temporary file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Save media file and generate URL using API_BASE_URL with retry
					mediaDir := filepath.Join("medias", txtid)
					err = createDirectoryWithRetry(mediaDir, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to create media directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate unique filename
					fileName := fmt.Sprintf("%s_%s%s", evt.Info.ID, fmt.Sprintf("%d", time.Now().Unix()), ext)
					mediaPath := filepath.Join(mediaDir, fileName)

					// Copy file to media directory with retry
					err = saveMediaWithRetry(data, mediaPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save media file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate URL using API_BASE_URL
					apiBaseURL := os.Getenv("API_BASE_URL")
					if apiBaseURL == "" {
						apiBaseURL = "http://localhost:8080"
					}
					mediaURL := fmt.Sprintf("%s/medias/%s/%s", apiBaseURL, txtid, fileName)

					// Add media info to postmap
					postmap["mediaUrl"] = mediaURL
					postmap["mimeType"] = audio.GetMimetype()
					postmap["fileName"] = fileName

					// Logar a URL gerada da mídia
					log.Info().Str("id", evt.Info.ID).Str("fileName", fileName).Str("mime", audio.GetMimetype()).Str("mediaUrl", mediaURL).Msg("Generated mediaUrl for received audio")

					// Convert the audio to base64 if needed (mantido para compatibilidade)
					{
						_, mimeType, err := fileToBase64(tmpPath)
						if err != nil {
							log.Error().Err(err).Msg("Failed to convert audio to base64")
							return
						}

						// Media saved to local directory and URL generated
						postmap["mimeType"] = mimeType
						postmap["fileName"] = filepath.Base(tmpPath)
					}

					// Log the successful conversion
					log.Info().Str("path", tmpPath).Msg("Audio processed")

					// Delete the temporary file
					err = os.Remove(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to delete temporary file")
					} else {
						log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
					}
				}

				// try to get Document if any
				document := evt.Message.GetDocumentMessage()
				if document != nil {
					// Create a temporary directory
					tmpDirectory := filepath.Join(os.TempDir(), "user_"+txtid)
					errDir := os.MkdirAll(tmpDirectory, os.ModePerm)
					if errDir != nil {
						log.Error().Err(errDir).Msg("Could not create temporary directory")
						return
					}

					// Download the document with retry
					data, err := downloadWhatsAppMediaWithRetry(mycli.WAClient, "document", document, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to download document after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Determine the file extension
					extension := ""
					exts, err := mime.ExtensionsByType(document.GetMimetype())
					if err == nil && len(exts) > 0 {
						extension = exts[0]
					} else {
						filename := document.FileName
						if filename != nil {
							extension = filepath.Ext(*filename)
						} else {
							extension = ".bin" // Default extension if no filename or MIME type is available
						}
					}
					tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+extension)

					// Write the document to the temporary file with retry
					err = saveMediaWithRetry(data, tmpPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save document to temporary file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Save media file and generate URL using API_BASE_URL with retry
					mediaDir := filepath.Join("medias", txtid)
					err = createDirectoryWithRetry(mediaDir, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to create media directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate unique filename
					fileName := fmt.Sprintf("%s_%s%s", evt.Info.ID, fmt.Sprintf("%d", time.Now().Unix()), extension)
					mediaPath := filepath.Join(mediaDir, fileName)

					// Copy file to media directory with retry
					err = saveMediaWithRetry(data, mediaPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save media file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate URL using API_BASE_URL
					apiBaseURL := os.Getenv("API_BASE_URL")
					if apiBaseURL == "" {
						apiBaseURL = "http://localhost:8080"
					}
					mediaURL := fmt.Sprintf("%s/medias/%s/%s", apiBaseURL, txtid, fileName)

					// Add media info to postmap
					postmap["mediaUrl"] = mediaURL
					postmap["mimeType"] = document.GetMimetype()
					postmap["fileName"] = fileName

					// Logar a URL gerada da mídia
					log.Info().Str("id", evt.Info.ID).Str("fileName", fileName).Str("mime", document.GetMimetype()).Str("mediaUrl", mediaURL).Msg("Generated mediaUrl for received document")

					// Convert the document to base64 if needed (mantido para compatibilidade)
					{
						_, mimeType, err := fileToBase64(tmpPath)
						if err != nil {
							log.Error().Err(err).Msg("Failed to convert document to base64")
							return
						}

						// Media saved to local directory and URL generated
						postmap["mimeType"] = mimeType
						postmap["fileName"] = filepath.Base(tmpPath)
					}

					// Log the successful conversion
					log.Info().Str("path", tmpPath).Msg("Document processed")

					// Delete the temporary file
					err = os.Remove(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to delete temporary file")
					} else {
						log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
					}
				}

				// try to get Video if any
				video := evt.Message.GetVideoMessage()
				if video != nil {
					// Create a temporary directory with retry
					tmpDirectory := filepath.Join(os.TempDir(), "user_"+txtid)
					errDir := createDirectoryWithRetry(tmpDirectory, 3)
					if errDir != nil {
						log.Error().Err(errDir).Msg("Could not create temporary directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Download the video with retry
					data, err := downloadWhatsAppMediaWithRetry(mycli.WAClient, "video", video, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to download video after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Determine the file extension based on the MIME type
					exts, _ := mime.ExtensionsByType(video.GetMimetype())
					var ext string
					if len(exts) > 0 {
						ext = exts[0]
					} else {
						ext = ".mp4" // Default extension for videos
					}
					tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+ext)

					// Write the video to the temporary file with retry
					err = saveMediaWithRetry(data, tmpPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save video to temporary file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Save media file and generate URL using API_BASE_URL with retry
					mediaDir := filepath.Join("medias", txtid)
					err = createDirectoryWithRetry(mediaDir, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to create media directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate unique filename
					fileName := fmt.Sprintf("%s_%s%s", evt.Info.ID, fmt.Sprintf("%d", time.Now().Unix()), ext)
					mediaPath := filepath.Join(mediaDir, fileName)

					// Copy file to media directory with retry
					err = saveMediaWithRetry(data, mediaPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save media file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate URL using API_BASE_URL
					apiBaseURL := os.Getenv("API_BASE_URL")
					if apiBaseURL == "" {
						apiBaseURL = "http://localhost:8080"
					}
					mediaURL := fmt.Sprintf("%s/medias/%s/%s", apiBaseURL, txtid, fileName)

					// Add media info to postmap
					postmap["mediaUrl"] = mediaURL
					postmap["mimeType"] = video.GetMimetype()
					postmap["fileName"] = fileName

					// Logar a URL gerada da mídia
					log.Info().Str("id", evt.Info.ID).Str("fileName", fileName).Str("mime", video.GetMimetype()).Str("mediaUrl", mediaURL).Msg("Generated mediaUrl for received video")

					// Convert the video to base64 if needed (mantido para compatibilidade)
					{
						_, mimeType, err := fileToBase64(tmpPath)
						if err != nil {
							log.Error().Err(err).Msg("Failed to convert video to base64")
							return
						}

						// Media saved to local directory and URL generated
						postmap["mimeType"] = mimeType
						postmap["fileName"] = filepath.Base(tmpPath)
					}

					// Log the successful conversion
					log.Info().Str("path", tmpPath).Msg("Video processed")

					// Delete the temporary file
					err = os.Remove(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to delete temporary file")
					} else {
						log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
					}
				}

				// try to get Sticker if any
				sticker := evt.Message.GetStickerMessage()
				if sticker != nil {
					// Create a temporary directory with retry
					tmpDirectory := filepath.Join(os.TempDir(), "user_"+txtid)
					errDir := createDirectoryWithRetry(tmpDirectory, 3)
					if errDir != nil {
						log.Error().Err(errDir).Msg("Could not create temporary directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Download the sticker with retry
					data, err := downloadWhatsAppMediaWithRetry(mycli.WAClient, "sticker", sticker, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to download sticker after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Determine the file extension based on the MIME type
					exts, _ := mime.ExtensionsByType(sticker.GetMimetype())
					var ext string
					if len(exts) > 0 {
						ext = exts[0]
					} else {
						ext = ".webp" // Default extension for stickers
					}
					tmpPath := filepath.Join(tmpDirectory, evt.Info.ID+ext)

					// Write the sticker to the temporary file with retry
					err = saveMediaWithRetry(data, tmpPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save sticker to temporary file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Save media file and generate URL using API_BASE_URL with retry
					mediaDir := filepath.Join("medias", txtid)
					err = createDirectoryWithRetry(mediaDir, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to create media directory after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate unique filename
					fileName := fmt.Sprintf("%s_%s%s", evt.Info.ID, fmt.Sprintf("%d", time.Now().Unix()), ext)
					mediaPath := filepath.Join(mediaDir, fileName)

					// Copy file to media directory with retry
					err = saveMediaWithRetry(data, mediaPath, 3)
					if err != nil {
						log.Error().Err(err).Msg("Failed to save media file after retries")
						// Continue without media processing
						goto sendWebhook
					}

					// Generate URL using API_BASE_URL
					apiBaseURL := os.Getenv("API_BASE_URL")
					if apiBaseURL == "" {
						apiBaseURL = "http://localhost:8080"
					}
					mediaURL := fmt.Sprintf("%s/medias/%s/%s", apiBaseURL, txtid, fileName)

					// Add media info to postmap
					postmap["mediaUrl"] = mediaURL
					postmap["mimeType"] = sticker.GetMimetype()
					postmap["fileName"] = fileName

					// Logar a URL gerada da mídia
					log.Info().Str("id", evt.Info.ID).Str("fileName", fileName).Str("mime", sticker.GetMimetype()).Str("mediaUrl", mediaURL).Msg("Generated mediaUrl for received sticker")

					// Convert the sticker to base64 if needed (mantido para compatibilidade)
					{
						_, mimeType, err := fileToBase64(tmpPath)
						if err != nil {
							log.Error().Err(err).Msg("Failed to convert sticker to base64")
							return
						}

						// Media saved to local directory and URL generated
						postmap["mimeType"] = mimeType
						postmap["fileName"] = filepath.Base(tmpPath)
					}

					// Log the successful conversion
					log.Info().Str("path", tmpPath).Msg("Sticker processed")

					// Delete the temporary file
					err = os.Remove(tmpPath)
					if err != nil {
						log.Error().Err(err).Msg("Failed to delete temporary file")
					} else {
						log.Info().Str("path", tmpPath).Msg("Temporary file deleted")
					}
				}
			}

		case *events.Receipt:
			postmap["type"] = "ReadReceipt"
			dowebhook = 1
			//if evt.Type == events.ReceiptTypeRead || evt.Type == events.ReceiptTypeReadSelf {
			if evt.Type == types.ReceiptTypeRead || evt.Type == types.ReceiptTypeReadSelf {
				log.Info().Strs("id", evt.MessageIDs).Str("source", evt.SourceString()).Str("timestamp", fmt.Sprintf("%v", evt.Timestamp)).Msg("Message was read")
				//if evt.Type == events.ReceiptTypeRead {
				if evt.Type == types.ReceiptTypeRead {
					postmap["state"] = "Read"
				} else {
					postmap["state"] = "ReadSelf"
				}
				//} else if evt.Type == events.ReceiptTypeDelivered {
			} else if evt.Type == types.ReceiptTypeDelivered {
				postmap["state"] = "Delivered"
				log.Info().Str("id", evt.MessageIDs[0]).Str("source", evt.SourceString()).Str("timestamp", fmt.Sprintf("%v", evt.Timestamp)).Msg("Message delivered")
			} else if evt.Type == types.ReceiptTypeSender {
				postmap["state"] = "Sender"
				log.Info().Str("id", evt.MessageIDs[0]).Str("source", evt.SourceString()).Str("timestamp", fmt.Sprintf("%v", evt.Timestamp)).Msg("Message delivered to sender's other devices")
			} else {
				// Discard webhooks for inactive or other delivery types
				return
			}
		case *events.Presence:
			postmap["type"] = "Presence"
			dowebhook = 1
			
			// ⭐ CORREÇÃO: Resolver LID para número real
			realFromNumber := resolveJIDToRealNumber(mycli, evt.From)
			postmap["from"] = realFromNumber
			
			if evt.Unavailable {
				postmap["state"] = "offline"
				if !evt.LastSeen.IsZero() {
					postmap["lastSeen"] = evt.LastSeen.Unix()
				}
				if evt.LastSeen.IsZero() {
					log.Info().
						Str("from", realFromNumber).
						Str("fromJID", evt.From.String()).
						Msg("✅ User is now offline")
				} else {
					log.Info().
						Str("from", realFromNumber).
						Str("fromJID", evt.From.String()).
						Str("lastSeen", fmt.Sprintf("%v", evt.LastSeen)).
						Msg("✅ User is now offline")
				}
			} else {
				postmap["state"] = "online"
				log.Info().
					Str("from", realFromNumber).
					Str("fromJID", evt.From.String()).
					Msg("✅ User is now online")
			}
		case *events.HistorySync:
			postmap["type"] = "HistorySync"

			// Histórico desativado - sempre pula o processamento
			log.Debug().
				Str("userID", mycli.userID).
				Msg("HistorySync event disabled - skipping history processing to avoid media download errors")

			// Não processa o histórico e não envia webhook
			// dowebhook = 1 (removido)
		case *events.AppState:
			log.Info().Str("index", fmt.Sprintf("%+v", evt.Index)).Str("actionValue", fmt.Sprintf("%+v", evt.SyncActionValue)).Msg("App state event received")
		case *events.LoggedOut:
			postmap["type"] = "Logged Out"
			dowebhook = 1
			log.Info().Str("reason", evt.Reason.String()).Msg("Logged out")
			killchannel[mycli.userID] <- true
			sqlStmt := `UPDATE users SET connected=0 WHERE id=$1`
			_, err := mycli.db.Exec(sqlStmt, mycli.userID)
			if err != nil {
				log.Error().Err(err).Msg(sqlStmt)
			}
		case *events.ChatPresence:
			postmap["type"] = "ChatPresence"
			dowebhook = 1
			
			// ⭐ CORREÇÃO: Usar getRealJIDWithFallback para resolver LID corretamente
			// ChatPresence tem MessageSource com Sender, SenderAlt e Chat
			
			// Resolver sender (quem está digitando/gravando)
			realSenderJID, senderLID := getRealJIDWithFallback(
				mycli,
				evt.MessageSource.Sender,     // primary
				evt.MessageSource.SenderAlt,  // alt
				evt.MessageSource.Chat,       // chatJID (para contexto)
			)
			
			// Salvar mapeamento LID se encontrado
			if !senderLID.IsEmpty() && !realSenderJID.IsEmpty() {
				saveLIDForUser(mycli, realSenderJID.User, senderLID.User)
			}
			
			// Resolver chat (onde está acontecendo)
			realChatJID := evt.MessageSource.Chat
			if isLID(realChatJID) {
				// Se chat for LID (raro mas possível), tentar resolver
				if phone, found := resolveLIDForUser(mycli, realChatJID.User); found {
					realChatJID = types.NewJID(phone, types.DefaultUserServer)
				}
			}
			
			// Determinar JIDs finais para o webhook
			var finalSenderStr, finalChatStr string
			
			if !realSenderJID.IsEmpty() {
				finalSenderStr = realSenderJID.String()
			} else {
				// Fallback: usar o sender original (pode ser LID)
				finalSenderStr = evt.MessageSource.Sender.String()
				log.Warn().
					Str("senderJID", evt.MessageSource.Sender.String()).
					Str("senderAlt", evt.MessageSource.SenderAlt.String()).
					Msg("⚠️  ChatPresence: Não foi possível resolver sender LID")
			}
			
			if !realChatJID.IsEmpty() {
				finalChatStr = realChatJID.String()
			} else {
				finalChatStr = evt.MessageSource.Chat.String()
			}
			
			// ⭐ Criar objeto event customizado com JIDs resolvidos
			eventData := map[string]interface{}{
				"Chat":      finalChatStr,
				"Sender":    finalSenderStr,
				"IsFromMe":  evt.MessageSource.IsFromMe,
				"IsGroup":   evt.MessageSource.IsGroup,
				"State":     fmt.Sprintf("%s", evt.State),
				"Media":     fmt.Sprintf("%s", evt.Media),
			}
			
			// Adicionar objeto customizado ao postmap
			postmap["event"] = eventData
			
			log.Info().
				Str("state", fmt.Sprintf("%s", evt.State)).
				Str("media", fmt.Sprintf("%s", evt.Media)).
				Str("chat", finalChatStr).
				Str("sender", finalSenderStr).
				Str("chatJID_original", evt.MessageSource.Chat.String()).
				Str("senderJID_original", evt.MessageSource.Sender.String()).
				Str("senderAlt", evt.MessageSource.SenderAlt.String()).
				Bool("senderResolved", !realSenderJID.IsEmpty()).
				Msg("✅ Chat Presence received")
		case *events.CallOffer:
			postmap["type"] = "CallOffer"
			postmap["callId"] = evt.CallID
			
			// ⭐ CORREÇÃO: Resolver LID para número real
			realFromNumber := resolveJIDToRealNumber(mycli, evt.From)
			postmap["from"] = realFromNumber
			postmap["timestamp"] = evt.Timestamp.Unix()

			callType := "voice"
			postmap["callType"] = callType
			dowebhook = 1
			log.Info().
				Str("callId", evt.CallID).
				Str("from", realFromNumber).
				Str("fromJID", evt.From.String()).
				Str("callType", callType).
				Msg("✅ Got call offer")
		case *events.CallAccept:
			log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call accept")
		case *events.CallTerminate:
			log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call terminate")
		case *events.CallOfferNotice:
			log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call offer notice")
		case *events.CallRelayLatency:
			log.Info().Str("event", fmt.Sprintf("%+v", evt)).Msg("Got call relay latency")
		case *events.Disconnected:
			postmap["type"] = "Disconnected"
			dowebhook = 1
			log.Info().Str("reason", fmt.Sprintf("%+v", evt)).Msg("Disconnected from Whatsapp")
		case *events.ConnectFailure:
			postmap["type"] = "ConnectFailure"
			dowebhook = 1
			log.Error().Str("reason", fmt.Sprintf("%+v", evt)).Msg("Failed to connect to Whatsapp")
		default:
			log.Warn().Str("event", fmt.Sprintf("%+v", evt)).Msg("Unhandled event")
		}

		// Label para fallback quando mídia falha
	sendWebhook:
		if dowebhook == 1 {
			// Enviar webhook (já está em goroutine separada)
			sendEventWithWebHook(mycli, postmap, path)
		}
	}() // Fechar a goroutine do evento
}
