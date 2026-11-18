# 单节点选举：重启恢复、自检与可观测性

本文档记录边缘场景下单节点 Raft 集群重启后的选举行为复核，配套回归测试
`single_node_election_test.go` 与选举诊断日志，便于后续评审与故障定位。

## 1. 背景与验收标准

边缘部署常见单节点集群。现象描述为：单节点重启后长期停留在 candidate 状态，
反复等待自身的选举超时，日志不再追加，客户端全部超时。

验收标准：

1. 单节点重启后能立即当选；
2. 投票请求能识别节点自己（自身投票不依赖网络自连）；
3. 恢复后能继续写日志；
4. 加入第二个节点后，选举与集群加入流程保持原行为；
5. 选举超时写入结构化日志，卡住时能判断卡在哪一步；
6. 现有选举参数（`HeartbeatTimeout`/`ElectionTimeout` 等）与集群加入流程不变。

## 2. 复核结论

复核基线为上游 `github.com/hashicorp/raft`
`v1.8.1-0.20261012063316-a5b469295ddc`（对应本仓库父提交 `a5b4692`）。
逐文件比对确认库源码与该基线一致，未发现会破坏单节点重启选举的注入缺陷。
单节点在所有标准恢复路径上都能立即当选，因此本次改动聚焦于**回归防护、
诊断可观测性与复核记录**，不改动任何选举参数、状态机分支或加入流程。

## 3. 关键代码路径（为什么单节点能立即当选）

### 3.1 配置恢复

重启时 `NewRaft` 通过两条路径恢复最新配置，保证自己仍在 voter 集合中：

- 配置日志条目仍在日志中：`NewRaft` 扫描配置条目并调用
  `processConfigurationLogEntry`（`api.go`）；
- 配置条目已被快照压缩：`restoreSnapshot` 从快照元数据恢复配置并调用
  `setLatestConfiguration` / `setCommittedConfiguration`（`api.go`）。

### 3.2 进入 candidate 与法定人数

- follower 心跳超时且自身有投票权时，`runFollower` 切换到 candidate；
  若 `latestIndex == 0` 或自身不在稳定配置中，则中止选举并输出明确告警。
- `quorumSize()` 对单个 voter 返回 `1/2 + 1 = 1`（`raft.go`），
  因此单节点只需自己一票即可胜选。

### 3.3 识别自己并自投票

`electSelf` / `preElectSelf` 遍历配置时通过 `server.ID == r.localID`
识别自身（`raft.go`）：

- 命中自身时不走任何网络 RPC，直接本地持久化投票
  （`persistVote`）并把赞成票放入响应 channel；
- 只有其他节点才通过 transport 发起 `RequestVote` / `RequestPreVote`。

因此单节点的胜选不依赖 transport 自连，地址变化或网络不可用都无法阻断自投票。
pre-vote 开启（默认）与关闭（`PreVoteDisabled`）两条路径行为一致。

### 3.4 恢复后继续写

胜选后进入 leader，单节点的 quorum 为 1，追加日志在本地落盘即满足提交条件，
客户端 `Apply` 正常返回；重启前的 term、last-vote 与日志均来自持久化存储，
任期与日志连续性得以保持。

## 4. 选举可观测性

仅新增结构化日志字段，不改变任何控制流。卡住时可据此判断所处阶段：

- `entering candidate state`：补充 `local-id`、`voters`、`votes-needed`、
  `last-log-index/term`、`election-timeout`、`pre-vote`，可确认自己是否在
  voter 集合、法定人数是多少、日志是否落后。
- `heartbeat timeout reached, starting election`：补充 `term`、`voters`、
  `votes-needed`、`election-timeout`。
- `Election timeout reached, restarting election`：补充本轮
  `votes-needed`、`pre-vote-granted/refused`、`votes-granted`、`voters`、
  `last-log-index`、`election-timeout`，可直接区分是票数不足、pre-vote 被拒、
  还是配置缺失。
- 两类中止选举告警（`no known peers`、`not part of stable configuration`）
  补充配置索引、voter 集合与超时值。

新增辅助函数 `voterIDs`（`configuration.go`）仅用于日志展示 voter 集合。

### 真实超时日志样例（隔离一个三节点候选者制造多数票缺失）

```
entering candidate state: ... term=3 local-id=nodeX voters=[nodeA,nodeB,nodeX]
  votes-needed=2 last-log-index=2 last-log-term=2 election-timeout=50ms pre-vote=true
Election timeout reached, restarting election: local-id=nodeX term=3
  votes-needed=2 pre-vote-granted=1 pre-vote-refused=2 votes-granted=0
  voters=[nodeA,nodeB,nodeX] last-log-index=2 election-timeout=50ms
```

该样例表明节点能识别自身（pre-vote 自带 1 票），但因拿不到第二个 voter
而超时，卡点与原因可直接从日志读出。

## 5. 回归测试与数据支撑

测试文件：`single_node_election_test.go`

| 测试 | 覆盖点 | 结果 |
| --- | --- | --- |
| `TestRaft_SingleNode_RestartElectsSelfAndWrites`（pre-vote 开/关） | 重启后立即当选、自投票识别、恢复后继续写且日志索引增长 | 通过 |
| `TestRaft_SingleNode_RestartAfterSnapshot` | 配置条目被快照压缩后，仍能从快照恢复配置并当选、继续写 | 通过 |
| `TestRaft_SingleNode_AddSecondNodeKeepsElectionBehavior`（pre-vote 开/关） | 标准 `AddVoter` 加入第二节点，原 leader 连任、新节点跟随、两节点 quorum 提交与复制一致 | 通过 |

当选耗时（测试配置 `HeartbeatTimeout=ElectionTimeout=50ms`，含随机退避）
实测约 58–100ms，与“等待一次心跳超时后立即凭自身一票胜选”的预期一致，
无多轮选举超时循环。

复现命令：

```
go test -run 'TestRaft_SingleNode_' -v -count=1 .
go test -race -run 'TestRaft_SingleNode_' -count=1 .
```

## 6. 不变量核对

- 未修改 `DefaultConfig`、`ValidateConfig` 及任何超时参数；
- 未修改 `runFollower`/`runCandidate`/`electSelf`/`preElectSelf` 的分支与
  状态转换，仅在既有日志调用上追加字段；
- 未修改 `AddVoter` 等成员变更流程，第二节点测试沿用标准加入路径；
- 新增 `voterIDs` 为纯只读展示函数，无副作用。
