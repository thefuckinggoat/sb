package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ─── Config Structs ───

type Config struct {
	Name         string                  `json:"name"`
	Version      string                  `json:"version"`
	IsDev        bool                    `json:"is_dev"`
	DeveloperIDs []string                `json:"developer_ids"`
	Tokens       []string                `json:"tokens"`
	UserSettings map[string]UserSettings `json:"user_settings"`
	DeepLAPIKey  string                  `json:"deepl_api_key,omitempty"`
}

type UserSettings struct {
	CommandPrefix string      `json:"command_prefix"`
	LeakcheckKey  string      `json:"leakcheck_api_key"`
	AutoDelete    AutoDelete  `json:"auto_delete"`
	Presence      Presence    `json:"presence"`
	Connected     bool        `json:"connected"`
	UID           int         `json:"uid"`
	DiscordID     int64       `json:"discord_id"`
	Username      string      `json:"username"`
	NitroSniper   NitroSniper `json:"nitro_sniper"`
}

type AutoDelete struct {
	Enabled bool `json:"enabled"`
	Delay   int  `json:"delay"`
}

type Presence struct {
	Type          string            `json:"type,omitempty"`
	Name          string            `json:"name,omitempty"`
	ApplicationID string            `json:"application_id,omitempty"`
	LargeImage    string            `json:"large_image,omitempty"`
	URL           string            `json:"url,omitempty"`
	Enabled       bool              `json:"enabled,omitempty"`
	State         string            `json:"state,omitempty"`
	Button1       string            `json:"button1,omitempty"`
	URL1          string            `json:"url1,omitempty"`
	PartySize     string            `json:"party_size,omitempty"`
	PartyMax      string            `json:"party_max,omitempty"`
	PartyID       string            `json:"party_id,omitempty"`
	Timestamp     int64             `json:"timestamp,omitempty"`
	CustomStatus  map[string]string `json:"custom_status,omitempty"`
}

type NitroSniper struct {
	Enabled bool `json:"enabled,omitempty"`
}

// ─── ConfigManager ───

type ConfigManager struct {
	mu       sync.RWMutex
	token    string
	config   Config
	uid      int
	botRef   *MyBot
	filePath string
}

func NewConfigManager(token string) *ConfigManager {
	cm := &ConfigManager{
		token:    token,
		filePath: "config.json",
	}
	cm.loadConfig()
	return cm
}

func (cm *ConfigManager) loadConfig() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	data, err := os.ReadFile(cm.filePath)
	if err != nil {
		return fmt.Errorf("error reading config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("error parsing config: %w", err)
	}
	cm.config = cfg
	if settings, ok := cfg.UserSettings[cm.token]; ok {
		cm.uid = settings.UID
	}
	return nil
}

func (cm *ConfigManager) ReloadConfig() error {
	return cm.loadConfig()
}

func (cm *ConfigManager) GetConfig() Config {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.config
}

func (cm *ConfigManager) GetUserSettings() (UserSettings, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	s, ok := cm.config.UserSettings[cm.token]
	return s, ok
}

func (cm *ConfigManager) GetCommandPrefix() string {
	s, ok := cm.GetUserSettings()
	if !ok || s.CommandPrefix == "" {
		return ">"
	}
	return s.CommandPrefix
}

func (cm *ConfigManager) SetUserConnected(uid int, connected bool) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for token, settings := range cm.config.UserSettings {
		if settings.UID == uid {
			settings.Connected = connected
			cm.config.UserSettings[token] = settings
			break
		}
	}
	return cm.saveConfigLocked()
}

func (cm *ConfigManager) UpdateUsername(token string, username string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if settings, ok := cm.config.UserSettings[token]; ok {
		settings.Username = username
		cm.config.UserSettings[token] = settings
		return cm.saveConfigLocked()
	}
	return nil
}

func (cm *ConfigManager) saveConfigLocked() error {
	data, err := json.MarshalIndent(cm.config, "", "    ")
	if err != nil {
		return fmt.Errorf("error marshaling config: %w", err)
	}
	return os.WriteFile(cm.filePath, data, 0644)
}

func (cm *ConfigManager) ValidateTokenAPI(token string) bool {
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", "https://discord.com/api/v9/users/@me", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", token)
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200
}

func (cm *ConfigManager) Cleanup() {}

func LoadConfigFile() (Config, error) {
	data, err := os.ReadFile("config.json")
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	return cfg, json.Unmarshal(data, &cfg)
}

// ─── AsyncWebSocket ───

type AsyncWebSocket struct {
	mu          sync.Mutex
	conn        *websocket.Conn
	closed      bool
	closeCode   int
	closeReason string
}

func NewAsyncWebSocket(conn *websocket.Conn) *AsyncWebSocket {
	return &AsyncWebSocket{conn: conn}
}

func (ws *AsyncWebSocket) IsClosed() bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.closed
}

func (ws *AsyncWebSocket) SendText(data string) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed {
		return fmt.Errorf("cannot send to closed websocket")
	}
	return ws.conn.WriteMessage(websocket.TextMessage, []byte(data))
}

func (ws *AsyncWebSocket) SendBytes(data []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed {
		return fmt.Errorf("cannot send to closed websocket")
	}
	return ws.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (ws *AsyncWebSocket) Receive() (int, []byte, error) {
	if ws.IsClosed() {
		return 0, nil, fmt.Errorf("cannot receive from closed websocket")
	}
	msgType, data, err := ws.conn.ReadMessage()
	if err != nil {
		ws.mu.Lock()
		ws.closed = true
		if closeErr, ok := err.(*websocket.CloseError); ok {
			ws.closeCode = closeErr.Code
			ws.closeReason = closeErr.Text
		}
		ws.mu.Unlock()
		return 0, nil, err
	}
	return msgType, data, nil
}

func (ws *AsyncWebSocket) Close(code int, reason string) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	log.Printf("[ws] Close called code=%d reason=%s", code, reason)
	ws.closeCode = code
	ws.closeReason = reason
	if !ws.closed {
		msg := websocket.FormatCloseMessage(code, reason)
		_ = ws.conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(5*time.Second))
		err := ws.conn.Close()
		ws.closed = true
		return err
	}
	return nil
}

func (ws *AsyncWebSocket) Ping() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed {
		return fmt.Errorf("cannot ping closed websocket")
	}
	return ws.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second))
}

func WSConnect(url string, headers map[string]string, timeout time.Duration) (*AsyncWebSocket, error) {
	dialer := websocket.Dialer{
		TLSClientConfig:  &tls.Config{MinVersion: tls.VersionTLS13},
		HandshakeTimeout: timeout,
	}
	httpHeaders := http.Header{}
	httpHeaders.Set("Accept-Language", "en-US,en;q=0.9")
	httpHeaders.Set("Accept-Encoding", "gzip, deflate, br, zstd")
	httpHeaders.Set("Origin", "https://discord.com")
	for k, v := range headers {
		httpHeaders.Set(k, v)
	}
	log.Printf("[ws] Connecting to %s", url)
	conn, _, err := dialer.Dial(url, httpHeaders)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to websocket: %w", err)
	}
	conn.SetReadLimit(8 * 1024 * 1024)
	log.Println("[ws] Connected successfully")
	return NewAsyncWebSocket(conn), nil
}

// ─── Browser Profiles & Spoofing ───

type BrowserProfile struct {
	Browser        string
	BrowserVersion string
	OSName         string
	OSVersion      string
	Device         string
	UserAgent      string
	BrowserUA      string
	ClientVersion  string
	SystemLocale   string
	ReleaseChannel string
}

var BrowserProfiles = map[string]BrowserProfile{
	"windows_stable": {
		Browser:        "Discord Client",
		BrowserVersion: "31.0.0",
		OSName:         "Windows",
		OSVersion:      "10.0.22631",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9168 Chrome/128.0.6613.186 Electron/31.0.0 Safari/537.36",
		BrowserUA:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.186 Safari/537.36",
		ClientVersion:  "1.0.9168",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
	"windows_canary": {
		Browser:        "Discord Client",
		BrowserVersion: "31.0.0",
		OSName:         "Windows",
		OSVersion:      "10.0.22631",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9169 Chrome/128.0.6613.186 Electron/31.0.0 Safari/537.36",
		BrowserUA:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.186 Safari/537.36",
		ClientVersion:  "1.0.9169",
		SystemLocale:   "en-US",
		ReleaseChannel: "canary",
	},
	"macos_stable": {
		Browser:        "Discord Client",
		BrowserVersion: "31.0.0",
		OSName:         "Mac OS X",
		OSVersion:      "14.5.0",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) discord/0.0.316 Chrome/128.0.6613.186 Electron/31.0.0 Safari/537.36",
		BrowserUA:      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.186 Safari/537.36",
		ClientVersion:  "0.0.316",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
	"linux_stable": {
		Browser:        "Discord Client",
		BrowserVersion: "31.0.0",
		OSName:         "Linux",
		OSVersion:      "6.5.0-44-generic",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) discord/0.0.71 Chrome/128.0.6613.186 Electron/31.0.0 Safari/537.36",
		BrowserUA:      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.186 Safari/537.36",
		ClientVersion:  "0.0.71",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
	"web_chrome": {
		Browser:        "Chrome",
		BrowserVersion: "128.0.0.0",
		OSName:         "Windows",
		OSVersion:      "10",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
		BrowserUA:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
		ClientVersion:  "",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
	"web_firefox": {
		Browser:        "Firefox",
		BrowserVersion: "129.0",
		OSName:         "Windows",
		OSVersion:      "10",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
		BrowserUA:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
		ClientVersion:  "",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
	"android": {
		Browser:        "Discord Android",
		BrowserVersion: "223.0 - rn",
		OSName:         "Android",
		OSVersion:      "14",
		Device:         "Google, Pixel 8 Pro",
		UserAgent:      "Discord-Android/223000;RNA",
		BrowserUA:      "Discord-Android/223000;RNA",
		ClientVersion:  "223.0",
		SystemLocale:   "en-US",
		ReleaseChannel: "googleRelease",
	},
	"ios": {
		Browser:        "Discord iOS",
		BrowserVersion: "223.0",
		OSName:         "iOS",
		OSVersion:      "17.5.1",
		Device:         "iPhone15,3",
		UserAgent:      "Discord-iOS/223000;iOS/17.5.1",
		BrowserUA:      "Discord-iOS/223000;iOS/17.5.1",
		ClientVersion:  "223.0",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
	"embedded": {
		Browser:        "Discord Embedded",
		BrowserVersion: "31.0.0",
		OSName:         "Windows",
		OSVersion:      "10.0.22631",
		Device:         "",
		UserAgent:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) discord/1.0.9168 Chrome/128.0.6613.186 Electron/31.0.0 Safari/537.36",
		BrowserUA:      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.186 Safari/537.36",
		ClientVersion:  "1.0.9168",
		SystemLocale:   "en-US",
		ReleaseChannel: "stable",
	},
}

type SuperProperties struct {
	OS                     string  `json:"os"`
	Browser                string  `json:"browser"`
	Device                 string  `json:"device"`
	SystemLocale           string  `json:"system_locale"`
	BrowserUserAgent       string  `json:"browser_user_agent"`
	BrowserVersion         string  `json:"browser_version"`
	OSVersion              string  `json:"os_version"`
	Referrer               string  `json:"referrer"`
	ReferringDomain        string  `json:"referring_domain"`
	ReferrerCurrent        string  `json:"referrer_current"`
	ReferringDomainCurrent string  `json:"referring_domain_current"`
	ReleaseChannel         string  `json:"release_channel"`
	ClientBuildNumber      int     `json:"client_build_number"`
	ClientEventSource      *string `json:"client_event_source"`
}

func BuildSuperProperties(profile BrowserProfile, buildNumber int) string {
	sp := SuperProperties{
		OS:                     profile.OSName,
		Browser:                profile.Browser,
		Device:                 profile.Device,
		SystemLocale:           profile.SystemLocale,
		BrowserUserAgent:       profile.BrowserUA,
		BrowserVersion:         profile.BrowserVersion,
		OSVersion:              profile.OSVersion,
		Referrer:               "",
		ReferringDomain:        "",
		ReferrerCurrent:        "",
		ReferringDomainCurrent: "",
		ReleaseChannel:         profile.ReleaseChannel,
		ClientBuildNumber:      buildNumber,
		ClientEventSource:      nil,
	}
	data, _ := json.Marshal(sp)
	return base64.StdEncoding.EncodeToString(data)
}

type ClientState struct {
	GuildVersions            map[string]interface{} `json:"guild_versions"`
	HighestLastMessageID     string                 `json:"highest_last_message_id"`
	ReadStateVersion         int                    `json:"read_state_version"`
	UserGuildSettingsVersion int                    `json:"user_guild_settings_version"`
	UserSettingsVersion      int                    `json:"user_settings_version"`
	PrivateChannelsVersion   string                 `json:"private_channels_version"`
	APICodeVersion           int                    `json:"api_code_version"`
}

func generateRandomUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ─── Gateway Constants & Structs ───

const (
	OpDispatch            = 0
	OpHeartbeat           = 1
	OpIdentify            = 2
	OpPresenceUpdate      = 3
	OpVoiceStateUpdate    = 4
	OpResume              = 6
	OpReconnect           = 7
	OpRequestGuildMembers = 8
	OpInvalidSession      = 9
	OpHello               = 10
	OpHeartbeatACK        = 11
)

type GatewayPayload struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d,omitempty"`
	S  *int            `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type HelloData struct {
	HeartbeatInterval int `json:"heartbeat_interval"`
}

type IdentifyData struct {
	Token        string                 `json:"token"`
	Capabilities int                    `json:"capabilities"`
	Properties   map[string]interface{} `json:"properties"`
	Presence     map[string]interface{} `json:"presence"`
	Compress     bool                   `json:"compress"`
	ClientState  ClientState            `json:"client_state"`
}

type ResumeData struct {
	Token     string `json:"token"`
	SessionID string `json:"session_id"`
	Seq       int    `json:"seq"`
}

type GatewayPresenceUpdate struct {
	Status     string     `json:"status"`
	Since      int        `json:"since"`
	Activities []Activity `json:"activities"`
	AFK        bool       `json:"afk"`
}

type Activity struct {
	Name          string            `json:"name"`
	Type          int               `json:"type"`
	URL           string            `json:"url,omitempty"`
	State         string            `json:"state,omitempty"`
	ApplicationID string            `json:"application_id,omitempty"`
	Assets        *ActivityAssets   `json:"assets,omitempty"`
	Buttons       []ActivityButton  `json:"buttons,omitempty"`
	Party         *ActivityParty    `json:"party,omitempty"`
	Timestamps    *ActivityTimestamp `json:"timestamps,omitempty"`
}

type ActivityAssets struct {
	LargeImage string `json:"large_image,omitempty"`
}

type ActivityButton struct {
	Label string `json:"label"`
	URL   string `json:"url"`
}

type ActivityParty struct {
	ID   string `json:"id,omitempty"`
	Size []int  `json:"size,omitempty"`
}

type ActivityTimestamp struct {
	Start int64 `json:"start,omitempty"`
}

type ReadyEvent struct {
	SessionID string   `json:"session_id"`
	User      DiscUser `json:"user"`
	ResumeURL string   `json:"resume_gateway_url"`
}

type DiscUser struct {
	ID            string `json:"id"`
	Username      string `json:"username"`
	Discriminator string `json:"discriminator"`
	Avatar        string `json:"avatar"`
}

type MessageCreate struct {
	ID        string   `json:"id"`
	ChannelID string   `json:"channel_id"`
	GuildID   string   `json:"guild_id,omitempty"`
	Content   string   `json:"content"`
	Author    DiscUser `json:"author"`
	Timestamp string   `json:"timestamp"`
}

// ─── GatewayConnection ───

type GatewayConnection struct {
	mu                sync.Mutex
	ws                *AsyncWebSocket
	token             string
	sessionID         string
	sequence          int
	resumeURL         string
	heartbeatInterval time.Duration
	lastHeartbeatSend time.Time
	lastHeartbeatAck  time.Time
	bot               *MyBot
	stopChan          chan struct{}
	closed            bool
	zBuf              bytes.Buffer
	profile           BrowserProfile
	buildNumber       int
	superProps        string
}

func NewGatewayConnection(bot *MyBot, token string) *GatewayConnection {
	profile := BrowserProfiles["windows_stable"]
	buildNumber := 313440

	return &GatewayConnection{
		token:       token,
		bot:         bot,
		stopChan:    make(chan struct{}),
		profile:     profile,
		buildNumber: buildNumber,
		superProps:  BuildSuperProperties(profile, buildNumber),
	}
}

func (gw *GatewayConnection) SetBrowserProfile(profileName string) bool {
	profile, ok := BrowserProfiles[profileName]
	if !ok {
		log.Printf("[gateway] Unknown browser profile: %s", profileName)
		return false
	}
	gw.mu.Lock()
	gw.profile = profile
	gw.superProps = BuildSuperProperties(profile, gw.buildNumber)
	gw.mu.Unlock()
	log.Printf("[gateway] Switched browser profile to: %s (%s on %s)", profileName, profile.Browser, profile.OSName)
	return true
}

func (gw *GatewayConnection) SetBuildNumber(buildNumber int) {
	gw.mu.Lock()
	gw.buildNumber = buildNumber
	gw.superProps = BuildSuperProperties(gw.profile, buildNumber)
	gw.mu.Unlock()
}

func (gw *GatewayConnection) Connect() error {
	url := "wss://gateway.discord.gg/?v=9&encoding=json&compress=zlib-stream"
	if gw.resumeURL != "" && gw.sessionID != "" {
		url = gw.resumeURL + "?v=9&encoding=json&compress=zlib-stream"
		log.Printf("[gateway] Attempting RESUME session_id=%s seq=%d", gw.sessionID, gw.sequence)
	}
	gw.mu.Lock()
	profile := gw.profile
	gw.mu.Unlock()

	headers := map[string]string{
		"User-Agent": profile.UserAgent,
	}
	ws, err := WSConnect(url, headers, 30*time.Second)
	if err != nil {
		return fmt.Errorf("gateway connect failed: %w", err)
	}
	gw.mu.Lock()
	gw.ws = ws
	gw.closed = false
	gw.mu.Unlock()
	return nil
}

func (gw *GatewayConnection) Run() error {
	if err := gw.Connect(); err != nil {
		return err
	}
	gw.zBuf.Reset()

	for {
		select {
		case <-gw.stopChan:
			return nil
		default:
		}

		_, data, err := gw.ws.Receive()
		if err != nil {
			if gw.isClosed() {
				return nil
			}
			log.Printf("[gateway] Receive error: %v", err)
			if err := gw.reconnect(); err != nil {
				return fmt.Errorf("reconnect failed: %w", err)
			}
			continue
		}

		gw.zBuf.Write(data)

		if len(data) < 4 ||
			data[len(data)-4] != 0x00 ||
			data[len(data)-3] != 0x00 ||
			data[len(data)-2] != 0xFF ||
			data[len(data)-1] != 0xFF {
			continue
		}

		reader, err := zlib.NewReader(&gw.zBuf)
		if err != nil {
			log.Printf("[gateway] zlib error: %v", err)
			gw.zBuf.Reset()
			continue
		}
		decompressed, err := io.ReadAll(reader)
		reader.Close()
		gw.zBuf.Reset()
		if err != nil {
			log.Printf("[gateway] zlib read error: %v", err)
			continue
		}

		var payload GatewayPayload
		if err := json.Unmarshal(decompressed, &payload); err != nil {
			log.Printf("[gateway] JSON parse error: %v", err)
			continue
		}

		if err := gw.handlePayload(&payload); err != nil {
			log.Printf("[gateway] Handle payload error: %v", err)
		}
	}
}

func (gw *GatewayConnection) handlePayload(payload *GatewayPayload) error {
	if payload.S != nil {
		gw.mu.Lock()
		gw.sequence = *payload.S
		gw.mu.Unlock()
	}

	switch payload.Op {
	case OpHello:
		var hello HelloData
		if err := json.Unmarshal(payload.D, &hello); err != nil {
			return fmt.Errorf("error parsing hello: %w", err)
		}
		gw.heartbeatInterval = time.Duration(hello.HeartbeatInterval) * time.Millisecond
		log.Printf("[gateway] Hello, heartbeat interval: %v", gw.heartbeatInterval)
		go gw.heartbeatLoop()
		if gw.sessionID != "" && gw.sequence > 0 {
			return gw.sendResume()
		}
		return gw.sendIdentify()

	case OpHeartbeat:
		return gw.sendHeartbeat()

	case OpHeartbeatACK:
		gw.mu.Lock()
		gw.lastHeartbeatAck = time.Now()
		lag := gw.lastHeartbeatAck.Sub(gw.lastHeartbeatSend)
		gw.mu.Unlock()
		log.Printf("[gateway] Heartbeat ACK latency=%v", lag)

	case OpDispatch:
		return gw.handleDispatch(payload)

	case OpReconnect:
		log.Println("[gateway] Received Reconnect")
		return gw.reconnect()

	case OpInvalidSession:
		var resumable bool
		json.Unmarshal(payload.D, &resumable)
		log.Printf("[gateway] Invalid session, resumable=%v", resumable)
		if !resumable {
			gw.mu.Lock()
			gw.sessionID = ""
			gw.sequence = 0
			gw.mu.Unlock()
		}
		time.Sleep(time.Duration(1+rand.Intn(4)) * time.Second)
		return gw.reconnect()
	}
	return nil
}

func (gw *GatewayConnection) handleDispatch(payload *GatewayPayload) error {
	switch payload.T {
	case "READY":
		var ready ReadyEvent
		if err := json.Unmarshal(payload.D, &ready); err != nil {
			return fmt.Errorf("error parsing READY: %w", err)
		}
		gw.mu.Lock()
		gw.sessionID = ready.SessionID
		gw.resumeURL = ready.ResumeURL
		gw.mu.Unlock()
		log.Printf("[gateway] READY as %s#%s (ID: %s)", ready.User.Username, ready.User.Discriminator, ready.User.ID)
		if gw.bot != nil {
			gw.bot.onReady(&ready)
		}
	case "RESUMED":
		log.Println("[gateway] RESUMED session")
	case "MESSAGE_CREATE":
		if gw.bot != nil {
			gw.bot.onMessageCreate(payload.D)
		}
	}
	if gw.bot != nil {
		gw.bot.dispatchEvent(payload.T, payload.D)
	}
	return nil
}

func (gw *GatewayConnection) sendIdentify() error {
	log.Printf("[gateway] Sending IDENTIFY (browser=%s, os=%s)", gw.profile.Browser, gw.profile.OSName)

	gw.mu.Lock()
	profile := gw.profile
	buildNumber := gw.buildNumber
	gw.mu.Unlock()

	properties := map[string]interface{}{
		"os":                       profile.OSName,
		"browser":                  profile.Browser,
		"device":                   profile.Device,
		"system_locale":            profile.SystemLocale,
		"browser_user_agent":       profile.BrowserUA,
		"browser_version":          profile.BrowserVersion,
		"os_version":               profile.OSVersion,
		"referrer":                 "",
		"referring_domain":         "",
		"referrer_current":         "",
		"referring_domain_current": "",
		"release_channel":          profile.ReleaseChannel,
		"client_build_number":      buildNumber,
		"client_event_source":      nil,
	}

	if profile.ClientVersion != "" {
		properties["client_version"] = profile.ClientVersion
	}

	if profile.OSName == "Android" || profile.OSName == "iOS" {
		properties["os_sdk_version"] = profile.OSVersion
		properties["device_vendor_id"] = generateRandomUUID()
	}

	identify := IdentifyData{
		Token:        gw.token,
		Capabilities: 16381,
		Properties:   properties,
		Presence: map[string]interface{}{
			"status":     "online",
			"since":      0,
			"activities": []interface{}{},
			"afk":        false,
		},
		Compress: false,
		ClientState: ClientState{
			GuildVersions:            map[string]interface{}{},
			HighestLastMessageID:     "0",
			ReadStateVersion:         0,
			UserGuildSettingsVersion: -1,
			UserSettingsVersion:      -1,
			PrivateChannelsVersion:   "0",
			APICodeVersion:           0,
		},
	}

	d, _ := json.Marshal(identify)
	return gw.send(GatewayPayload{Op: OpIdentify, D: d})
}

func (gw *GatewayConnection) sendResume() error {
	log.Printf("[gateway] Sending RESUME session_id=%s seq=%d", gw.sessionID, gw.sequence)
	data := ResumeData{
		Token:     gw.token,
		SessionID: gw.sessionID,
		Seq:       gw.sequence,
	}
	d, _ := json.Marshal(data)
	return gw.send(GatewayPayload{Op: OpResume, D: d})
}

func (gw *GatewayConnection) sendHeartbeat() error {
	gw.mu.Lock()
	seq := gw.sequence
	gw.lastHeartbeatSend = time.Now()
	gw.mu.Unlock()
	d, _ := json.Marshal(seq)
	return gw.send(GatewayPayload{Op: OpHeartbeat, D: d})
}

func (gw *GatewayConnection) heartbeatLoop() {
	if gw.heartbeatInterval == 0 {
		return
	}
	jitter := time.Duration(float64(gw.heartbeatInterval) * (rand.Float64() * 0.5))
	time.Sleep(jitter)

	ticker := time.NewTicker(gw.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gw.stopChan:
			return
		case <-ticker.C:
			if gw.isClosed() {
				return
			}
			gw.mu.Lock()
			lastAck := gw.lastHeartbeatAck
			lastSend := gw.lastHeartbeatSend
			gw.mu.Unlock()
			if !lastSend.IsZero() && !lastAck.IsZero() && time.Since(lastAck) > 2*gw.heartbeatInterval {
				log.Printf("[gateway] Heartbeat stalled (last ACK %v ago), forcing reconnect", time.Since(lastAck))
				gw.ws.Close(1011, "heartbeat stalled")
				return
			}
			if err := gw.sendHeartbeat(); err != nil {
				log.Printf("[gateway] Heartbeat send error: %v", err)
				return
			}
		}
	}
}

func (gw *GatewayConnection) send(payload GatewayPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return gw.ws.SendText(string(data))
}

func (gw *GatewayConnection) reconnect() error {
	log.Println("[gateway] Reconnecting...")
	if gw.ws != nil && !gw.ws.IsClosed() {
		gw.ws.Close(1000, "reconnecting")
	}
	time.Sleep(2 * time.Second)
	if err := gw.Connect(); err != nil {
		return err
	}
	gw.zBuf.Reset()
	return nil
}

func (gw *GatewayConnection) isClosed() bool {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	return gw.closed || (gw.ws != nil && gw.ws.IsClosed())
}

func (gw *GatewayConnection) GatewayClose(code int, reason string) error {
	gw.mu.Lock()
	gw.closed = true
	gw.mu.Unlock()
	select {
	case <-gw.stopChan:
	default:
		close(gw.stopChan)
	}
	if gw.ws != nil {
		return gw.ws.Close(code, reason)
	}
	return nil
}

func (gw *GatewayConnection) UpdatePresence(presence *GatewayPresenceUpdate) error {
	d, _ := json.Marshal(presence)
	return gw.send(GatewayPayload{Op: OpPresenceUpdate, D: d})
}

func (gw *GatewayConnection) Watchdog() {
	const heartbeatStallSeconds = 90
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-gw.stopChan:
			return
		case <-ticker.C:
			if gw.isClosed() {
				return
			}
			gw.mu.Lock()
			lastAck := gw.lastHeartbeatAck
			lastSend := gw.lastHeartbeatSend
			gw.mu.Unlock()
			now := time.Now()
			stalledByAck := !lastAck.IsZero() && now.Sub(lastAck).Seconds() > heartbeatStallSeconds
			stalledNoAck := !lastSend.IsZero() && lastAck.Before(lastSend) && now.Sub(lastSend).Seconds() > heartbeatStallSeconds
			if stalledByAck || stalledNoAck {
				lag := now.Sub(lastAck)
				if lastAck.IsZero() {
					lag = now.Sub(lastSend)
				}
				log.Printf("[watchdog] Heartbeat stalled (%v). Forcing reconnect.", lag)
				gw.ws.Close(1011, "watchdog reconnect")
			}
		}
	}
}

// ─── MyBot ───

type CommandHandler func(bot *MyBot, msg *MessageCreate, args []string) error
type EventHandler func(bot *MyBot, data json.RawMessage)

type Cog interface {
	Name() string
	Register(bot *MyBot)
}

type MyBot struct {
	mu            sync.RWMutex
	token         string
	configManager *ConfigManager
	manager       *BotManager
	db            *DatabaseManager
	gateway       *GatewayConnection
	startTime     time.Time
	autoDelete    bool
	closed        bool
	user          *DiscUser
	commands      map[string]CommandHandler
	eventHandlers map[string][]EventHandler
	cogs          map[string]Cog
	prefix        string
}

type DatabaseManager struct{}

func NewDatabaseManager() *DatabaseManager { return &DatabaseManager{} }
func (d *DatabaseManager) Initialize() error { return nil }
func (d *DatabaseManager) Close()            {}

func NewMyBot(token string, manager *BotManager) *MyBot {
	cm := NewConfigManager(token)
	bot := &MyBot{
		token:         token,
		configManager: cm,
		manager:       manager,
		startTime:     time.Now(),
		commands:      make(map[string]CommandHandler),
		eventHandlers: make(map[string][]EventHandler),
		cogs:          make(map[string]Cog),
		prefix:        cm.GetCommandPrefix(),
	}
	cm.botRef = bot
	settings, ok := cm.GetUserSettings()
	if ok {
		bot.autoDelete = settings.AutoDelete.Enabled
	}
	return bot
}

func (b *MyBot) Start() error {
	log.Printf("[bot] Starting with token %s...", b.token[:10])

	if !b.configManager.ValidateTokenAPI(b.token) {
		log.Printf("[bot] Skipping invalid token: %s...", b.token[:10])
		return fmt.Errorf("invalid token")
	}

	b.db = NewDatabaseManager()
	if err := b.db.Initialize(); err != nil {
		log.Printf("[bot] Warning: Database init failed: %v", err)
		b.db = nil
	}

	b.loadCogs()

	b.gateway = NewGatewayConnection(b, b.token)
	go b.gateway.Watchdog()

	for {
		if b.IsClosed() {
			return nil
		}
		err := b.gateway.Run()
		if err != nil {
			log.Printf("[bot %s] Gateway error: %v", b.token[:10], err)
			if b.IsClosed() {
				return nil
			}
			if isAuthError(err) {
				b.handleInvalidToken(true)
				return err
			}
			log.Printf("[bot %s] Reconnecting in 5s...", b.token[:10])
			time.Sleep(5 * time.Second)
			continue
		}
	}
}

func (b *MyBot) onReady(ready *ReadyEvent) {
	b.mu.Lock()
	b.user = &ready.User
	b.mu.Unlock()
	log.Printf("[bot] Logged in as %s#%s (ID: %s)", ready.User.Username, ready.User.Discriminator, ready.User.ID)
	b.configManager.UpdateUsername(b.token, ready.User.Username)

	settings, ok := b.configManager.GetUserSettings()
	if ok && settings.Presence.Enabled {
		b.setPresence(&settings.Presence)
	}
}

func (b *MyBot) onMessageCreate(data json.RawMessage) {
	var msg MessageCreate
	if err := json.Unmarshal(data, &msg); err != nil {
		log.Printf("[bot] Error parsing MESSAGE_CREATE: %v", err)
		return
	}

	if b.user == nil || msg.Author.ID != b.user.ID {
		return
	}

	if len(msg.Content) < len(b.prefix) || msg.Content[:len(b.prefix)] != b.prefix {
		return
	}

	content := msg.Content[len(b.prefix):]
	args := splitArgs(content)
	if len(args) == 0 {
		return
	}

	cmdName := strings.ToLower(args[0])
	cmdArgs := args[1:]

	b.mu.RLock()
	handler, ok := b.commands[cmdName]
	b.mu.RUnlock()

	if ok {
		go func() {
			if err := handler(b, &msg, cmdArgs); err != nil {
				log.Printf("[bot] Command error (%s): %v", cmdName, err)
			}
			if b.autoDelete {
				settings, ok := b.configManager.GetUserSettings()
				if ok {
					time.Sleep(time.Duration(settings.AutoDelete.Delay) * time.Second)
					b.deleteMessage(msg.ChannelID, msg.ID)
				}
			}
		}()
	}
}

func (b *MyBot) dispatchEvent(eventName string, data json.RawMessage) {
	b.mu.RLock()
	handlers, ok := b.eventHandlers[eventName]
	b.mu.RUnlock()
	if ok {
		for _, handler := range handlers {
			go handler(b, data)
		}
	}
}

func (b *MyBot) RegisterCommand(name string, handler CommandHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands[name] = handler
}

func (b *MyBot) RegisterEventHandler(event string, handler EventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.eventHandlers[event] = append(b.eventHandlers[event], handler)
}

func (b *MyBot) RegisterCog(cog Cog) {
	b.mu.Lock()
	b.cogs[cog.Name()] = cog
	b.mu.Unlock()
	cog.Register(b)
}

func (b *MyBot) loadCogs() {
	// Register your cogs here
	log.Printf("[bot %s] Loaded %d cogs with %d commands", b.token[:10], len(b.cogs), len(b.commands))
}

func (b *MyBot) setPresence(p *Presence) {
	if b.gateway == nil {
		return
	}
	actType := 0
	switch p.Type {
	case "playing":
		actType = 0
	case "streaming":
		actType = 1
	case "listening":
		actType = 2
	case "watching":
		actType = 3
	case "competing":
		actType = 5
	}
	activity := Activity{
		Name:          p.Name,
		Type:          actType,
		URL:           p.URL,
		State:         p.State,
		ApplicationID: p.ApplicationID,
	}
	if p.LargeImage != "" {
		activity.Assets = &ActivityAssets{LargeImage: p.LargeImage}
	}
	if p.Button1 != "" && p.URL1 != "" {
		activity.Buttons = []ActivityButton{{Label: p.Button1, URL: p.URL1}}
	}
	if p.PartyID != "" {
		ps, _ := strconv.Atoi(p.PartySize)
		pm, _ := strconv.Atoi(p.PartyMax)
		activity.Party = &ActivityParty{ID: p.PartyID}
		if ps > 0 && pm > 0 {
			activity.Party.Size = []int{ps, pm}
		}
	}
	if p.Timestamp > 0 {
		activity.Timestamps = &ActivityTimestamp{Start: time.Now().UnixMilli() - (p.Timestamp * 1000)}
	}
	presence := &GatewayPresenceUpdate{
		Status:     "online",
		Since:      0,
		Activities: []Activity{activity},
		AFK:        false,
	}
	if err := b.gateway.UpdatePresence(presence); err != nil {
		log.Printf("[bot] Error setting presence: %v", err)
	}
}

func (b *MyBot) handleInvalidToken(markDisconnected bool) {
	if b.IsClosed() {
		return
	}
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	if markDisconnected {
		log.Printf("[bot %s] Marking token as permanently disconnected", b.token[:10])
		b.configManager.ReloadConfig()
		b.configManager.SetUserConnected(b.configManager.uid, false)
	}
	b.BotClose()
}

func (b *MyBot) BotClose() {
	log.Println("[bot] Starting shutdown...")
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	if b.gateway != nil {
		b.gateway.GatewayClose(1000, "shutting down")
	}
	if b.db != nil {
		b.db.Close()
		b.db = nil
	}
	if b.configManager != nil {
		b.configManager.Cleanup()
	}
	log.Println("[bot] Shutdown complete")
}

func (b *MyBot) IsClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

func (b *MyBot) getSpoofedHeaders() (BrowserProfile, string) {
	if b.gateway != nil {
		b.gateway.mu.Lock()
		profile := b.gateway.profile
		superProps := b.gateway.superProps
		b.gateway.mu.Unlock()
		return profile, superProps
	}
	profile := BrowserProfiles["windows_stable"]
	return profile, BuildSuperProperties(profile, 313440)
}

func (b *MyBot) apiRequest(method, path string, body interface{}) error {
	url := "https://discord.com/api/v9" + path
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return err
	}

	profile, superProps := b.getSpoofedHeaders()

	req.Header.Set("Authorization", b.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", profile.UserAgent)
	req.Header.Set("X-Super-Properties", superProps)
	req.Header.Set("X-Discord-Locale", profile.SystemLocale)
	req.Header.Set("X-Discord-Timezone", "America/New_York")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Sec-Ch-Ua", fmt.Sprintf(`"Chromium";v="%s", "Not;A=Brand";v="24"`, profile.BrowserVersion))
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", fmt.Sprintf(`"%s"`, profile.OSName))
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/channels/@me")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (b *MyBot) SendMessage(channelID string, content string) error {
	return b.apiRequest("POST", fmt.Sprintf("/channels/%s/messages", channelID), map[string]string{"content": content})
}

func (b *MyBot) SendEmbed(channelID string, embed map[string]interface{}) error {
	return b.apiRequest("POST", fmt.Sprintf("/channels/%s/messages", channelID), map[string]interface{}{"embeds": []map[string]interface{}{embed}})
}

func (b *MyBot) EditMessage(channelID, messageID, content string) error {
	return b.apiRequest("PATCH", fmt.Sprintf("/channels/%s/messages/%s", channelID, messageID), map[string]string{"content": content})
}

func (b *MyBot) deleteMessage(channelID, messageID string) error {
	return b.apiRequest("DELETE", fmt.Sprintf("/channels/%s/messages/%s", channelID, messageID), nil)
}

func (b *MyBot) GetUser(userID string) (*DiscUser, error) {
	url := fmt.Sprintf("https://discord.com/api/v9/users/%s", userID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	profile, superProps := b.getSpoofedHeaders()

	req.Header.Set("Authorization", b.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", profile.UserAgent)
	req.Header.Set("X-Super-Properties", superProps)
	req.Header.Set("X-Discord-Locale", profile.SystemLocale)
	req.Header.Set("Accept", "*/*")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to fetch user: status %d", resp.StatusCode)
	}
	var user DiscUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}
	return &user, nil
}

func (b *MyBot) UpdateBrowserProperty(profileName string) bool {
	if b.gateway == nil {
		log.Println("[bot] Cannot update browser: no gateway connection")
		return false
	}
	if !b.gateway.SetBrowserProfile(profileName) {
		return false
	}
	log.Printf("[bot] Updated browser profile to: %s, forcing reconnect...", profileName)
	if b.gateway.ws != nil && !b.gateway.ws.IsClosed() {
		b.gateway.ws.Close(1000, "browser profile change")
	}
	return true
}

func GetAvailableProfiles() []string {
	profiles := make([]string, 0, len(BrowserProfiles))
	for name := range BrowserProfiles {
		profiles = append(profiles, name)
	}
	return profiles
}

// ─── HotReloader ───

type HotReloader struct {
	manager    *BotManager
	signalFile string
	lastCheck  time.Time
	running    bool
	mu         sync.Mutex
	stopChan   chan struct{}
}

func NewHotReloader(manager *BotManager) *HotReloader {
	return &HotReloader{
		manager:    manager,
		signalFile: ".hot_reload_signal",
		stopChan:   make(chan struct{}),
	}
}

func (hr *HotReloader) StartMonitoring() {
	hr.mu.Lock()
	hr.running = true
	hr.mu.Unlock()
	go hr.monitorLoop()
	log.Println("[hotreload] Monitor started")
}

func (hr *HotReloader) StopMonitoring() {
	hr.mu.Lock()
	hr.running = false
	hr.mu.Unlock()
	select {
	case <-hr.stopChan:
	default:
		close(hr.stopChan)
	}
	log.Println("[hotreload] Monitor stopped")
}

func (hr *HotReloader) monitorLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-hr.stopChan:
			return
		case <-ticker.C:
			hr.checkSignalFile()
		}
	}
}

func (hr *HotReloader) checkSignalFile() {
	info, err := os.Stat(hr.signalFile)
	if err != nil {
		return
	}
	modTime := info.ModTime()
	if !modTime.After(hr.lastCheck) {
		return
	}
	hr.lastCheck = modTime

	data, err := os.ReadFile(hr.signalFile)
	if err != nil {
		return
	}

	content := strings.TrimSpace(string(data))
	if strings.HasPrefix(content, "HOT_RELOAD:") {
		parts := strings.SplitN(content, ":", 3)
		if len(parts) == 3 {
			signalType := parts[1]
			target := parts[2]
			log.Printf("[hotreload] Processing signal: %s:%s", signalType, target)
			log.Printf("[hotreload] Signal received for %s: %s (requires supervisor restart for Go)", signalType, target)
		}
	}
}

// ─── BotManager ───

type BotManager struct {
	mu                 sync.RWMutex
	bots               map[string]*MyBot
	sharedInsults      []string
	sharedWords        map[string]struct{}
	sharedRizzLines    []string
	sharedSkibidiLines []string
	hotReloader        *HotReloader
	sharedConfig       *ConfigManager
}

func NewBotManager() *BotManager {
	bm := &BotManager{
		bots:        make(map[string]*MyBot),
		sharedWords: make(map[string]struct{}),
	}

	bm.sharedConfig = NewConfigManager("")

	bm.sharedInsults = loadFileLines("config/insults.txt", false)
	bm.sharedRizzLines = loadFileLines("config/rizz.txt", false)
	bm.sharedSkibidiLines = loadFileLines("config/skibidi.txt", false)
	words := loadFileLines("config/blacktea.txt", true)
	for _, w := range words {
		bm.sharedWords[w] = struct{}{}
	}

	log.Printf("[manager] Loaded shared data: %d insults, %d words, %d rizz, %d skibidi",
		len(bm.sharedInsults), len(bm.sharedWords), len(bm.sharedRizzLines), len(bm.sharedSkibidiLines))

	if os.Getenv("HOT_RELOAD_ENABLED") == "1" {
		bm.hotReloader = NewHotReloader(bm)
	}

	return bm
}

func (bm *BotManager) LoadTokens() []string {
	cfg := bm.sharedConfig.GetConfig()
	var tokens []string
	for _, token := range cfg.Tokens {
		if token == "" {
			continue
		}
		if settings, ok := cfg.UserSettings[token]; ok {
			if settings.Connected {
				tokens = append(tokens, token)
			}
		}
	}
	return tokens
}

func (bm *BotManager) StartAll(ctx context.Context) error {
	log.Println("[manager] Initializing...")

	tokens := bm.LoadTokens()
	if len(tokens) == 0 {
		return fmt.Errorf("no valid tokens found")
	}

	log.Printf("[manager] Starting %d bot instances concurrently...", len(tokens))

	var wg sync.WaitGroup
	for _, token := range tokens {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			bm.startBot(t)
		}(token)
	}

	log.Printf("[manager] Started %d bot instances", len(tokens))

	if bm.hotReloader != nil {
		bm.hotReloader.StartMonitoring()
	}

	go func() {
		<-ctx.Done()
		bm.Cleanup()
	}()

	wg.Wait()
	return nil
}

func (bm *BotManager) startBot(token string) {
	if !bm.sharedConfig.ValidateTokenAPI(token) {
		log.Printf("[manager] Skipping invalid token: %s...", token[:10])
		return
	}

	bot := NewMyBot(token, bm)

	bm.mu.Lock()
	bm.bots[token] = bot
	bm.mu.Unlock()

	if err := bot.Start(); err != nil {
		log.Printf("[manager] Bot %s failed: %v", token[:10], err)
		bm.mu.Lock()
		delete(bm.bots, token)
		bm.mu.Unlock()
	}
}

func (bm *BotManager) Cleanup() {
	log.Println("[manager] Starting cleanup...")

	if bm.hotReloader != nil {
		bm.hotReloader.StopMonitoring()
	}

	bm.mu.RLock()
	bots := make([]*MyBot, 0, len(bm.bots))
	for _, bot := range bm.bots {
		bots = append(bots, bot)
	}
	bm.mu.RUnlock()

	var wg sync.WaitGroup
	for _, bot := range bots {
		wg.Add(1)
		go func(b *MyBot) {
			defer wg.Done()
			b.BotClose()
		}(bot)
	}
	wg.Wait()

	if bm.sharedConfig != nil {
		bm.sharedConfig.Cleanup()
	}

	log.Println("[manager] Cleanup completed")
}

// ─── Utility Functions ───

func loadFileLines(path string, lower bool) []string {
	file, err := os.Open(path)
	if err != nil {
		log.Printf("[util] File not found: %s", path)
		return nil
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			if lower {
				line = strings.ToLower(line)
			}
			lines = append(lines, line)
		}
	}
	log.Printf("[util] Loaded %d items from %s", len(lines), path)
	return lines
}

func splitArgs(s string) []string {
	return strings.Fields(s)
}

func isAuthError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Authentication") || strings.Contains(s, "401")
}

// ─── main() ───

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	logFile, err := os.OpenFile("bot.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("Warning: could not open log file: %v", err)
	} else {
		defer logFile.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	manager := NewBotManager()

	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	if err := manager.StartAll(ctx); err != nil {
		log.Fatalf("Fatal error: %v", err)
	}

	<-ctx.Done()
	log.Println("Initiating shutdown sequence")
	manager.Cleanup()
	log.Println("Shutdown complete")
}