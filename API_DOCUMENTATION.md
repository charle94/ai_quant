# API 文档

## 概述

量化分析系统提供了RESTful API接口，用于获取交易信号、特征数据和系统状态。

## 基础信息

- **决策引擎API**: `http://localhost:8080`
- **Feast Serving API**: `http://localhost:6566`
- **数据格式**: JSON
- **字符编码**: UTF-8

## 🎯 决策引擎API

### 健康检查

**GET** `/health`

检查决策引擎服务状态。

**响应示例**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**状态码**:
- `200`: 服务正常
- `500`: 服务异常

---

### 获取最新交易信号

**GET** `/signals`

获取所有交易对的最新交易信号。

**查询参数**:
- `limit` (可选): 返回信号数量，默认10

**请求示例**:
```bash
curl "http://localhost:8080/signals?limit=5"
```

**响应示例**:
```json
{
  "signals": [
    {
      "trading_pair": "BTCUSDT",
      "signal": "BUY",
      "price": 45000.0,
      "buy_score": 6,
      "sell_score": 2,
      "trend": "UP",
      "momentum_signal": "STRONG_UP",
      "volume_signal": "HIGH",
      "risk_level": "MEDIUM",
      "position_size": 0.7,
      "timestamp": "2024-01-15T10:30:00Z",
      "features": {
        "rsi_14": 65.5,
        "ma_5": 44950.0,
        "ma_10": 44900.0,
        "volatility": 0.02,
        "volume_ratio": 1.2,
        "momentum_5d": 0.01
      }
    }
  ],
  "count": 1
}
```

**状态码**:
- `200`: 成功
- `500`: 服务器错误

---

### 获取指定交易对信号

**GET** `/signals/{pair}`

获取指定交易对的历史交易信号。

**路径参数**:
- `pair`: 交易对名称 (如: BTCUSDT)

**查询参数**:
- `limit` (可选): 返回信号数量，默认5

**请求示例**:
```bash
curl "http://localhost:8080/signals/BTCUSDT?limit=3"
```

**响应示例**:
```json
{
  "trading_pair": "BTCUSDT",
  "signals": [
    {
      "signal": "BUY",
      "price": 45000.0,
      "buy_score": 6,
      "sell_score": 2,
      "timestamp": "2024-01-15T10:30:00Z"
    }
  ],
  "count": 1
}
```

---

### 手动触发决策

**POST** `/trigger`

手动触发一次决策计算。

**请求示例**:
```bash
curl -X POST "http://localhost:8080/trigger"
```

**响应示例**:
```json
{
  "message": "决策已触发"
}
```

## 🍽️ Feast Serving API

### 获取在线特征

**POST** `/get-online-features`

从Feast获取指定实体的在线特征。

**请求体**:
```json
{
  "features": [
    "realtime_features:price",
    "realtime_features:rsi_14",
    "realtime_features:ma_5"
  ],
  "entity_rows": [
    {"trading_pair": "BTCUSDT"},
    {"trading_pair": "ETHUSDT"}
  ],
  "full_feature_names": false
}
```

**请求示例**:
```bash
curl -X POST "http://localhost:6566/get-online-features" \
  -H "Content-Type: application/json" \
  -d '{
    "features": [
      "realtime_features:price",
      "realtime_features:rsi_14"
    ],
    "entity_rows": [
      {"trading_pair": "BTCUSDT"}
    ]
  }'
```

**响应示例**:
```json
{
  "feature_names": ["price", "rsi_14"],
  "results": [
    {
      "values": [45000.0, 65.5],
      "statuses": ["PRESENT", "PRESENT"],
      "event_timestamps": ["2024-01-15T10:30:00Z", "2024-01-15T10:30:00Z"]
    }
  ]
}
```

---

### Feast健康检查

**GET** `/health`

检查Feast服务状态。

**响应示例**:
```json
{
  "status": "healthy"
}
```

## 📊 数据模型

### 交易信号 (TradingSignal)

| 字段 | 类型 | 描述 |
|------|------|------|
| trading_pair | string | 交易对名称 |
| signal | string | 交易信号 (BUY/SELL/HOLD) |
| price | float | 当前价格 |
| buy_score | integer | 买入评分 (0-10) |
| sell_score | integer | 卖出评分 (0-10) |
| trend | string | 趋势方向 (UP/DOWN/SIDEWAYS) |
| momentum_signal | string | 动量信号 (STRONG_UP/STRONG_DOWN/WEAK) |
| volume_signal | string | 成交量信号 (HIGH/LOW/NORMAL) |
| risk_level | string | 风险等级 (LOW/MEDIUM/HIGH) |
| position_size | float | 建议仓位比例 (0-1) |
| timestamp | string | 信号生成时间 (ISO 8601) |
| features | object | 相关特征数据 |

### 特征数据 (Features)

| 字段 | 类型 | 描述 |
|------|------|------|
| price | float | 当前价格 |
| volume | integer | 成交量 |
| ma_5 | float | 5期移动平均 |
| ma_10 | float | 10期移动平均 |
| rsi_14 | float | 14期RSI |
| volatility | float | 波动率 |
| volume_ratio | float | 成交量比率 |
| momentum_5d | float | 5日动量 |

## 🔄 信号类型说明

### 交易信号 (Signal)

- **BUY**: 买入信号，建议开多仓
- **SELL**: 卖出信号，建议开空仓或平多仓
- **HOLD**: 持有信号，维持当前仓位

### 趋势信号 (Trend)

- **UP**: 上升趋势，价格呈上涨态势
- **DOWN**: 下降趋势，价格呈下跌态势
- **SIDEWAYS**: 横盘整理，价格在区间内震荡

### 动量信号 (Momentum)

- **STRONG_UP**: 强烈上涨动量
- **STRONG_DOWN**: 强烈下跌动量
- **WEAK**: 动量较弱

### 成交量信号 (Volume)

- **HIGH**: 高成交量，通常伴随重要价格变动
- **LOW**: 低成交量，市场活跃度较低
- **NORMAL**: 正常成交量水平

### 风险等级 (Risk Level)

- **LOW**: 低风险，建议满仓操作
- **MEDIUM**: 中等风险，建议70%仓位
- **HIGH**: 高风险，建议50%仓位

## 🚨 错误处理

### 错误响应格式

```json
{
  "error": {
    "code": "ERROR_CODE",
    "message": "错误描述",
    "details": "详细信息"
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### 常见错误码

| 错误码 | HTTP状态码 | 描述 |
|--------|------------|------|
| INVALID_TRADING_PAIR | 400 | 无效的交易对 |
| FEATURE_NOT_FOUND | 404 | 特征不存在 |
| SERVICE_UNAVAILABLE | 503 | 服务不可用 |
| INTERNAL_ERROR | 500 | 内部服务器错误 |

## 📝 使用示例

### Python客户端示例

```python
import requests
import json

class QuantAPIClient:
    def __init__(self, base_url="http://localhost:8080"):
        self.base_url = base_url
    
    def get_signals(self, trading_pair=None, limit=10):
        """获取交易信号"""
        if trading_pair:
            url = f"{self.base_url}/signals/{trading_pair}"
        else:
            url = f"{self.base_url}/signals"
        
        params = {"limit": limit}
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"API请求失败: {response.status_code}")
    
    def trigger_decision(self):
        """手动触发决策"""
        url = f"{self.base_url}/trigger"
        response = requests.post(url)
        return response.json()

# 使用示例
client = QuantAPIClient()

# 获取所有信号
all_signals = client.get_signals()
print(f"获取到 {all_signals['count']} 个信号")

# 获取BTCUSDT信号
btc_signals = client.get_signals("BTCUSDT", limit=5)
print(f"BTCUSDT最新信号: {btc_signals['signals'][0]['signal']}")

# 触发决策
result = client.trigger_decision()
print(result['message'])
```

### JavaScript客户端示例

```javascript
class QuantAPIClient {
    constructor(baseUrl = 'http://localhost:8080') {
        this.baseUrl = baseUrl;
    }
    
    async getSignals(tradingPair = null, limit = 10) {
        const url = tradingPair 
            ? `${this.baseUrl}/signals/${tradingPair}?limit=${limit}`
            : `${this.baseUrl}/signals?limit=${limit}`;
        
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`API请求失败: ${response.status}`);
        }
        
        return await response.json();
    }
    
    async triggerDecision() {
        const response = await fetch(`${this.baseUrl}/trigger`, {
            method: 'POST'
        });
        
        return await response.json();
    }
}

// 使用示例
const client = new QuantAPIClient();

// 获取信号
client.getSignals('BTCUSDT', 3)
    .then(data => {
        console.log(`获取到 ${data.count} 个BTCUSDT信号`);
        console.log('最新信号:', data.signals[0]);
    })
    .catch(error => {
        console.error('错误:', error);
    });
```

### Shell脚本示例

```bash
#!/bin/bash

API_BASE="http://localhost:8080"

# 获取健康状态
echo "检查服务健康状态..."
curl -s "${API_BASE}/health" | jq '.'

# 获取最新信号
echo "获取最新交易信号..."
curl -s "${API_BASE}/signals?limit=3" | jq '.signals[] | {trading_pair, signal, price, timestamp}'

# 获取BTCUSDT信号
echo "获取BTCUSDT信号..."
curl -s "${API_BASE}/signals/BTCUSDT" | jq '.signals[0]'

# 触发决策
echo "触发手动决策..."
curl -s -X POST "${API_BASE}/trigger" | jq '.'
```

## 🔐 认证和安全

目前系统为演示版本，未实现认证机制。生产环境建议添加：

### API密钥认证
```bash
curl -H "X-API-Key: your-api-key" "http://localhost:8080/signals"
```

### JWT认证
```bash
curl -H "Authorization: Bearer your-jwt-token" "http://localhost:8080/signals"
```

### IP白名单
在配置文件中限制访问IP:
```yaml
security:
  allowed_ips:
    - "127.0.0.1"
    - "192.168.1.0/24"
```

## 📈 性能建议

1. **批量请求**: 尽量使用批量API而非单次请求
2. **缓存结果**: 客户端应适当缓存不频繁变化的数据
3. **连接复用**: 使用HTTP连接池
4. **请求限制**: 避免过于频繁的API调用

## 📞 技术支持

如有API使用问题，请：
1. 查看本文档
2. 检查服务日志
3. 提交GitHub Issue
4. 联系技术支持

---

**注意**: 本API文档基于当前版本，后续版本可能会有变动，请关注更新日志。