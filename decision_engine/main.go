package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"gopkg.in/yaml.v3"
)

// Config 配置结构
type Config struct {
	Server struct {
		Port         string `yaml:"port"`
		ReadTimeout  int    `yaml:"read_timeout"`
		WriteTimeout int    `yaml:"write_timeout"`
	} `yaml:"server"`
	Feast struct {
		BaseURL    string   `yaml:"base_url"`
		UseMock    bool     `yaml:"use_mock"`
		TradingPairs []string `yaml:"trading_pairs"`
	} `yaml:"feast"`
	Trading struct {
		UpdateInterval int `yaml:"update_interval"`
		RulesFile     string `yaml:"rules_file"`
	} `yaml:"trading"`
}

// TradingSignal 交易信号
type TradingSignal struct {
	TradingPair   string                 `json:"trading_pair"`
	Signal        string                 `json:"signal"`
	Price         float64               `json:"price"`
	BuyScore      int                   `json:"buy_score"`
	SellScore     int                   `json:"sell_score"`
	Trend         string                `json:"trend"`
	MomentumSignal string               `json:"momentum_signal"`
	VolumeSignal  string                `json:"volume_signal"`
	RiskLevel     string                `json:"risk_level"`
	PositionSize  float64               `json:"position_size"`
	Timestamp     string                `json:"timestamp"`
	Features      map[string]interface{} `json:"features"`
}

// DecisionEngine 决策引擎
type DecisionEngine struct {
	config      *Config
	feastClient interface{} // 可以是 *FeastClient 或 *MockFeastClient
	ruleEngine  types.RuleEngine
	signals     chan TradingSignal
	isRunning   bool
}

// NewDecisionEngine 创建决策引擎
func NewDecisionEngine(configFile string) (*DecisionEngine, error) {
	// 加载配置
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, fmt.Errorf("加载配置失败: %v", err)
	}

	// 创建Feast客户端
	var feastClient interface{}
	if config.Feast.UseMock {
		feastClient = NewMockFeastClient()
		log.Println("使用模拟Feast客户端")
	} else {
		feastClient = NewFeastClient(config.Feast.BaseURL)
		log.Println("使用真实Feast客户端")
	}

	// 初始化RuleGo引擎
	ruleEngine := rulego.New()

	// 加载交易规则
	err = loadTradingRules(ruleEngine, config.Trading.RulesFile)
	if err != nil {
		return nil, fmt.Errorf("加载交易规则失败: %v", err)
	}

	return &DecisionEngine{
		config:      config,
		feastClient: feastClient,
		ruleEngine:  ruleEngine,
		signals:     make(chan TradingSignal, 100),
		isRunning:   false,
	}, nil
}

// loadConfig 加载配置文件
func loadConfig(configFile string) (*Config, error) {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// loadTradingRules 加载交易规则
func loadTradingRules(engine types.RuleEngine, rulesFile string) error {
	data, err := ioutil.ReadFile(rulesFile)
	if err != nil {
		return err
	}

	// 创建规则链
	_, err = engine.NewRuleChain(data)
	if err != nil {
		return fmt.Errorf("创建规则链失败: %v", err)
	}

	log.Println("交易规则加载成功")
	return nil
}

// Start 启动决策引擎
func (de *DecisionEngine) Start() {
	de.isRunning = true
	
	// 启动特征获取和决策循环
	go de.decisionLoop()
	
	// 启动HTTP服务器
	de.startHTTPServer()
}

// Stop 停止决策引擎
func (de *DecisionEngine) Stop() {
	de.isRunning = false
	close(de.signals)
}

// decisionLoop 决策循环
func (de *DecisionEngine) decisionLoop() {
	ticker := time.NewTicker(time.Duration(de.config.Trading.UpdateInterval) * time.Second)
	defer ticker.Stop()

	for de.isRunning {
		select {
		case <-ticker.C:
			de.processDecision()
		}
	}
}

// processDecision 处理决策
func (de *DecisionEngine) processDecision() {
	// 获取特征数据
	var features map[string]*TradingFeatures
	var err error

	switch client := de.feastClient.(type) {
	case *FeastClient:
		features, err = client.GetOnlineFeatures(de.config.Feast.TradingPairs)
	case *MockFeastClient:
		features, err = client.GetOnlineFeatures(de.config.Feast.TradingPairs)
	default:
		log.Printf("未知的Feast客户端类型")
		return
	}

	if err != nil {
		log.Printf("获取特征数据失败: %v", err)
		return
	}

	// 为每个交易对生成决策
	for pair, feature := range features {
		signal := de.generateTradingSignal(pair, feature)
		if signal != nil {
			select {
			case de.signals <- *signal:
				log.Printf("生成交易信号: %s - %s", pair, signal.Signal)
			default:
				log.Printf("信号队列已满，丢弃信号: %s", pair)
			}
		}
	}
}

// generateTradingSignal 生成交易信号
func (de *DecisionEngine) generateTradingSignal(tradingPair string, features *TradingFeatures) *TradingSignal {
	// 构建消息数据
	msgData := map[string]interface{}{
		"trading_pair":  features.TradingPair,
		"price":         features.Price,
		"ma_5":          features.MA5,
		"ma_10":         features.MA10,
		"rsi_14":        features.RSI14,
		"volatility":    features.Volatility,
		"volume_ratio":  features.VolumeRatio,
		"momentum_5d":   features.Momentum5D,
		"timestamp":     features.Timestamp.Format(time.RFC3339),
	}

	// 转换为JSON
	msgBytes, err := json.Marshal(msgData)
	if err != nil {
		log.Printf("序列化消息数据失败: %v", err)
		return nil
	}

	// 创建RuleGo消息
	msg := types.NewMsg(0, "FEATURE_DATA", types.JSON, msgBytes)

	// 设置回调函数来捕获结果
	var result *TradingSignal
	
	// 执行规则链
	de.ruleEngine.OnMsg(msg, types.WithEndFunc(func(ctx types.RuleContext, msg types.RuleMsg, err error) {
		if err != nil {
			log.Printf("规则执行错误: %v", err)
			return
		}

		// 解析结果
		var signalData TradingSignal
		if err := json.Unmarshal([]byte(msg.Data), &signalData); err != nil {
			log.Printf("解析交易信号失败: %v", err)
			return
		}

		result = &signalData
	}))

	return result
}

// startHTTPServer 启动HTTP服务器
func (de *DecisionEngine) startHTTPServer() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// 健康检查
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "healthy",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})

	// 获取最新信号
	r.GET("/signals", func(c *gin.Context) {
		signals := de.getRecentSignals(10)
		c.JSON(http.StatusOK, gin.H{
			"signals": signals,
			"count":   len(signals),
		})
	})

	// 获取指定交易对的信号
	r.GET("/signals/:pair", func(c *gin.Context) {
		pair := c.Param("pair")
		signals := de.getSignalsForPair(pair, 5)
		c.JSON(http.StatusOK, gin.H{
			"trading_pair": pair,
			"signals":      signals,
			"count":        len(signals),
		})
	})

	// 手动触发决策
	r.POST("/trigger", func(c *gin.Context) {
		go de.processDecision()
		c.JSON(http.StatusOK, gin.H{
			"message": "决策已触发",
		})
	})

	// 启动服务器
	srv := &http.Server{
		Addr:         ":" + de.config.Server.Port,
		Handler:      r,
		ReadTimeout:  time.Duration(de.config.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(de.config.Server.WriteTimeout) * time.Second,
	}

	log.Printf("HTTP服务器启动在端口 %s", de.config.Server.Port)
	
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP服务器启动失败: %v", err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("正在关闭服务器...")

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("服务器关闭超时:", err)
	}

	log.Println("服务器已关闭")
}

// getRecentSignals 获取最近的信号
func (de *DecisionEngine) getRecentSignals(limit int) []TradingSignal {
	signals := make([]TradingSignal, 0, limit)
	
	// 从信号通道中读取信号（非阻塞）
	for i := 0; i < limit; i++ {
		select {
		case signal := <-de.signals:
			signals = append(signals, signal)
		default:
			break
		}
	}
	
	return signals
}

// getSignalsForPair 获取指定交易对的信号
func (de *DecisionEngine) getSignalsForPair(pair string, limit int) []TradingSignal {
	allSignals := de.getRecentSignals(50) // 获取更多信号来筛选
	
	signals := make([]TradingSignal, 0, limit)
	for _, signal := range allSignals {
		if signal.TradingPair == pair {
			signals = append(signals, signal)
			if len(signals) >= limit {
				break
			}
		}
	}
	
	return signals
}

func main() {
	// 检查配置文件参数
	configFile := "/workspace/config/rulego.yml"
	if len(os.Args) > 1 {
		configFile = os.Args[1]
	}

	// 创建决策引擎
	engine, err := NewDecisionEngine(configFile)
	if err != nil {
		log.Fatalf("创建决策引擎失败: %v", err)
	}

	log.Println("量化决策引擎启动中...")
	
	// 启动引擎
	engine.Start()
}