# var-lighter

Variational–Lighter 永续合约跨所对冲脚本，采用 MIT 许可证。

保留 Variational 与 Lighter 的行情接入、跨所开平仓、分段策略、风控和终端监控。已移除其他交易所适配器、独立网格、单所刷量、网页管理、部署和诊断工具。

## 获取与安装

```sh
git clone https://github.com/baalisgood/var-lighter.git
cd var-lighter
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-py312.txt
cp env.template .env
```

仓库仅包含空白鉴权模板。`.env`、日志、账户数据、抓包和本地备份不应上传；填写过的 `config/exchanges/*_config.yaml` 也含敏感信息，分享或提交前须恢复为空白模板。忽略规则不能保护已经被 Git 跟踪的文件。

## 配置自己的账户

1. 使用 Python 3.12，创建自己的虚拟环境并安装 `requirements-py312.txt`。
2. 将 `env.template` 复制为 `.env`，填写自己的 Lighter API 签名私钥和账户索引（不是钱包助记词或钱包私钥）、VAR 浏览器鉴权信息。两份 `config/exchanges/*_config.yaml` 是空白模板，不含原作者凭证。
3. 在浏览器中加载 `tools/variational_token_helper_extension/`，获取自己账号的鉴权信息。插件本身不是下单模板，也不保证鉴权永久有效。
4. VAR 配置已恢复六类历史请求模板：市价下单、开放订单、历史订单、历史成交、余额和持仓查询，以及账户轮询/WS 参数；模板不含原作者的鉴权头、Cookie 或账户标识。使用自己的浏览器核对这些请求是否仍有效；如需更新，可用插件的“复制 trade YAML”功能或 `tools/variational_import_curl.py`。限价及撤单模板没有从原配置中恢复，因此本交付版的 VAR 下单配置应保持市价模式。不要将原始抓包或已填写配置分享出去。
5. 检查策略中的标的、数量、资金上限、手续费、滑点、交易模式和 `system_mode.monitor_only`。模板中的数值不是针对接收者资金规模的新推荐。

## 唯一配置模板集

- `config/arbitrage/arbitrage_segmented.yaml`：分段策略。
- `config/arbitrage/arbitrage_unified.yaml`：执行方式、手续费、风控及普通 V3 策略。
- `config/arbitrage/monitor_v2.yaml`：VAR/Lighter 行情监控。
- `config/arbitrage/multi_exchange_arbitrage.yaml`：VAR/Lighter 配对。
- `config/exchanges/lighter_config.yaml`、`variational_config.yaml`：两边的空白鉴权模板；VAR 另包含脱敏后的请求结构。

监控、配对和分段策略统一保留 BTC、ETH、BNB、SOL、HYPE。`symbol_conversion.yaml`、`logging.yaml` 和 `lighter_markets.json` 是共享基础配置/数据，不是重复的交易模板。

## 运行入口

查看行情（不执行订单）：

```sh
python run_arbitrage_monitor_v2.py --config config/arbitrage/monitor_v2.yaml
```

分段对冲入口（完成自己的鉴权、模板和参数验证后使用）：

```sh
python main_unified.py \
  --config config/arbitrage/arbitrage_segmented.yaml \
  --monitor-config config/arbitrage/monitor_v2.yaml \
  --multi-exchange-config config/arbitrage/multi_exchange_arbitrage.yaml
```

三个路径已经与默认入口对齐。普通 V3 套利入口为 `run_arbitrage_execution_v3.py`。

两份执行策略的 `system_mode.monitor_only` 默认均为 `true`，不会提交真实订单。分段入口读取 `arbitrage_segmented.yaml` 的模式，普通 V3 入口读取 `arbitrage_unified.yaml` 的模式。完成自己的报价、鉴权、模板和预算验证后，再明确修改对应配置为实盘模式。

保持可见前台运行。分段入口收到 Ctrl+C 时，原有退出逻辑可能尝试紧急平仓；短时间内再次 Ctrl+C 会直接退出、不平仓。不要把停止程序理解成已经平掉仓位。

## 历史离线验证范围

以下是交付时记录的历史验证，不代表本次公开发布重新执行了这些业务验证；当前仓库未附带当时的测试脚本。

- 三个入口的 `--help` 可以加载。
- 本地模块引用完整，工厂仅注册 Lighter 和 VAR。
- 虚构订单下，两种对冲方向的开仓/平仓路由通过；监控模式不提交订单。
- 使用恢复后的脱敏模板和虚构 HTTP 响应，验证了 VAR 买卖下单、余额、持仓、历史订单成交确认及历史成交查询；没有访问真实交易接口。

未使用真实账户、未访问交易接口、未实盘发单。空白配置必须由使用者补齐；离线验证不能保证当前交易所接口、鉴权和真实成交正常，也不保证盈利或零损耗。

## 许可证

Copyright (c) 2026 baalisgood。代码依据 [MIT 许可证](LICENSE) 开源，按现状提供，不保证盈利或适合实盘使用。
