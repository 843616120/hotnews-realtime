# 测试数据

`tests/` 是普通测试资料目录，不需要 `pom.xml`。

建议结构：

```text
tests/
├── inputs/
├── expected/
├── cases/
└── README.md
```

测试至少覆盖正常输入、乱序、迟到、重复、非法 JSON、缺失字段、Join 未匹配、TTL 过期、阈值边界和热点 Key 倾斜。
