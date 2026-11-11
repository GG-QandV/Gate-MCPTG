# Fix: `ack()` не удалял сообщения из store при пустом RAM-буфере

Дата: 2026-11-11. Релиз: v0.9.4+. Файл: `src/mcp_telegram/inbox.py`.

## Симптом

Штатное удаление сообщения через IPC (`inbox_ack` с `last_id`) возвращает
`{"dropped": 0}`, а `inbox_peek` продолжает отдавать это сообщение.
Повторные ack'и не помогают — сообщение «неудаляемо», инбокс не чистится.

## Корневая причина

```python
async def ack(self, chat_id, topic_id, last_id):
    key = (chat_id, topic_id)
    async with self._lock:
        buf = self._buffers.get(key)
        if not buf:
            return 0            # ← выход ДО обращения к store
        ...
    store_dropped = await self.store.ack(chat_id, topic_id, last_id)
```

Ранний `return` срабатывает всегда, когда RAM-буфер пуст, а это нормальный
режим после рестарта демона: `peek()` тогда читает сообщения напрямую из
store (`InboxStore.read_all`), буфер остаётся пустым, и `store.ack()` не
вызывается **никогда**. Сообщения из `inbox_store/*.jsonl` становились
неудаляемыми: они возвращались каждым следующим `peek`.

## Исправление

`store.ack()` вызывается всегда; работа с RAM-буфером выполняется только
если он непустой:

```python
async with self._lock:
    buf = self._buffers.get(key)
    if buf:
        self._buffers[key] = deque(
            (m for m in buf if m["id"] > last_id), maxlen=self.maxlen
        )
store_dropped = await self.store.ack(chat_id, topic_id, last_id)
```

## Регресс-тест

`tests/unit/test_inbox.py::test_ack_cleans_store_when_ram_buffer_empty`
— пишет 3 сообщения прямо в store (буфер пуст, как после рестарта), вызывает
`ack(last_id=1)` и требует `dropped == 1` и `[2, 3]` в `peek`.

Тест проверен «в обе стороны»: на коде до фикса падает (`AssertionError`),
после фикса проходит. Полный набор `tests/unit/test_inbox.py` — 23 passed.

## Проверка на живом демоне

```
store до ack:   15 строк
ack 1887     -> {'dropped': 1}
peek         -> 14 сообщений, 1887 отсутствует
store после:    14 строк
```

## Как обнаружилось

При диагностике Mnemostroma (см. `mnemostroma/docs/ops/2026-11-11-stability-fix.md`)
понадобилось удалить устаревшее сообщение из инбокса. `inbox_ack` вернул
`dropped: 0`, сообщение осталось — расхождение между заявленным контрактом
(`ack` удаляет ≤ `last_id`) и фактическим поведением.
