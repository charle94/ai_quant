package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-resty/resty/v2"
)

// FeastClient Feast客户端
type FeastClient struct {
	BaseURL string
	Client  *resty.Client
}

// FeatureRequest 特征请求结构
type FeatureRequest struct {
	Features    []string                 `json:"features"`
	EntityRows  []map[string]interface{} `json:"entity_rows"`
	FullFeatureNames bool                `json:"full_feature_names,omitempty"`
}

// FeatureResponse 特征响应结构
type FeatureResponse struct {
	FeatureNames []string        `json:"feature_names"`
	Results      []FeatureResult `json:"results"`
}

// FeatureResult 单个特征结果
type FeatureResult struct {
	Values    []interface{} `json:"values"`
	Statuses  []string     `json:"statuses"`
	EventTimestamps []string `json:"event_timestamps"`
}

// TradingFeatures 交易特征数据
type TradingFeatures struct {
	TradingPair     string    `json:"trading_pair"`
	Price          float64   `json:"price"`
	MA5            float64   `json:"ma_5"`
	MA10           float64   `json:"ma_10"`
	RSI14          float64   `json:"rsi_14"`
	Volatility     float64   `json:"volatility"`
	VolumeRatio    float64   `json:"volume_ratio"`
	Momentum5D     float64   `json:"momentum_5d"`
	Timestamp      time.Time `json:"timestamp"`
}

// NewFeastClient 创建新的Feast客户端
func NewFeastClient(baseURL string) *FeastClient {
	client := resty.New().
		SetTimeout(10 * time.Second).
		SetRetryCount(3).
		SetRetryWaitTime(1 * time.Second)

	return &FeastClient{
		BaseURL: baseURL,
		Client:  client,
	}
}

// GetOnlineFeatures 获取在线特征
func (fc *FeastClient) GetOnlineFeatures(tradingPairs []string) (map[string]*TradingFeatures, error) {
	// 构建实体行
	entityRows := make([]map[string]interface{}, len(tradingPairs))
	for i, pair := range tradingPairs {
		entityRows[i] = map[string]interface{}{
			"trading_pair": pair,
		}
	}

	// 构建请求
	request := FeatureRequest{
		Features: []string{
			"realtime_features:price",
			"realtime_features:ma_5",
			"realtime_features:ma_10",
			"realtime_features:rsi_14",
			"realtime_features:volatility",
			"realtime_features:volume_ratio",
			"realtime_features:momentum_5d",
		},
		EntityRows:       entityRows,
		FullFeatureNames: false,
	}

	// 发送请求
	resp, err := fc.Client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(request).
		Post(fc.BaseURL + "/get-online-features")

	if err != nil {
		return nil, fmt.Errorf("请求Feast服务失败: %v", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("Feast服务返回错误状态: %d, 响应: %s", resp.StatusCode(), resp.String())
	}

	// 解析响应
	var featureResp FeatureResponse
	if err := json.Unmarshal(resp.Body(), &featureResp); err != nil {
		return nil, fmt.Errorf("解析Feast响应失败: %v", err)
	}

	// 转换为交易特征格式
	result := make(map[string]*TradingFeatures)
	
	for i, pair := range tradingPairs {
		if i >= len(featureResp.Results) {
			continue
		}

		result := &featureResp.Results[i]
		if len(result.Values) < 7 {
			log.Printf("警告: %s 的特征数据不完整", pair)
			continue
		}

		features := &TradingFeatures{
			TradingPair: pair,
			Timestamp:   time.Now(),
		}

		// 安全地转换特征值
		if val, ok := result.Values[0].(float64); ok {
			features.Price = val
		}
		if val, ok := result.Values[1].(float64); ok {
			features.MA5 = val
		}
		if val, ok := result.Values[2].(float64); ok {
			features.MA10 = val
		}
		if val, ok := result.Values[3].(float64); ok {
			features.RSI14 = val
		}
		if val, ok := result.Values[4].(float64); ok {
			features.Volatility = val
		}
		if val, ok := result.Values[5].(float64); ok {
			features.VolumeRatio = val
		}
		if val, ok := result.Values[6].(float64); ok {
			features.Momentum5D = val
		}

		result[pair] = features
	}

	return result, nil
}

// GetHistoricalFeatures 获取历史特征（用于回测）
func (fc *FeastClient) GetHistoricalFeatures(tradingPairs []string, startTime, endTime time.Time) ([]map[string]*TradingFeatures, error) {
	// 这里可以实现历史特征获取逻辑
	// 由于Feast的历史特征API比较复杂，这里提供一个基础框架
	log.Printf("获取历史特征: %v, 时间范围: %v - %v", tradingPairs, startTime, endTime)
	
	// 返回空结果，实际实现需要根据具体的Feast配置
	return []map[string]*TradingFeatures{}, nil
}

// HealthCheck 健康检查
func (fc *FeastClient) HealthCheck() error {
	resp, err := fc.Client.R().Get(fc.BaseURL + "/health")
	if err != nil {
		return fmt.Errorf("Feast健康检查失败: %v", err)
	}

	if resp.StatusCode() != 200 {
		return fmt.Errorf("Feast服务不健康, 状态码: %d", resp.StatusCode())
	}

	return nil
}

// MockFeastClient 模拟Feast客户端（用于测试）
type MockFeastClient struct{}

// NewMockFeastClient 创建模拟客户端
func NewMockFeastClient() *MockFeastClient {
	return &MockFeastClient{}
}

// GetOnlineFeatures 模拟获取在线特征
func (mfc *MockFeastClient) GetOnlineFeatures(tradingPairs []string) (map[string]*TradingFeatures, error) {
	result := make(map[string]*TradingFeatures)

	for _, pair := range tradingPairs {
		var price, ma5, ma10, rsi14, volatility, volumeRatio, momentum5d float64

		// 根据不同交易对生成不同的模拟数据
		switch pair {
		case "BTCUSDT":
			price = 45000 + float64(time.Now().Unix()%1000-500)
			ma5 = price * 0.998
			ma10 = price * 0.995
			rsi14 = 50 + float64(time.Now().Unix()%40-20)
			volatility = 0.02
			volumeRatio = 1.2
			momentum5d = 0.01
		case "ETHUSDT":
			price = 2500 + float64(time.Now().Unix()%100-50)
			ma5 = price * 0.999
			ma10 = price * 0.997
			rsi14 = 50 + float64(time.Now().Unix()%30-15)
			volatility = 0.03
			volumeRatio = 1.1
			momentum5d = -0.005
		default:
			price = 1000
			ma5 = price
			ma10 = price
			rsi14 = 50
			volatility = 0.01
			volumeRatio = 1.0
			momentum5d = 0.0
		}

		result[pair] = &TradingFeatures{
			TradingPair: pair,
			Price:       price,
			MA5:         ma5,
			MA10:        ma10,
			RSI14:       rsi14,
			Volatility:  volatility,
			VolumeRatio: volumeRatio,
			Momentum5D:  momentum5d,
			Timestamp:   time.Now(),
		}
	}

	return result, nil
}

// HealthCheck 模拟健康检查
func (mfc *MockFeastClient) HealthCheck() error {
	return nil
}