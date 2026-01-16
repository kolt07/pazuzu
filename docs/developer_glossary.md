# Developer glossary

## Terms

- **status**: статус тендера/аукціону в OpenProcurement/ProZorro (наприклад: `active`, `active.tendering`, `active.auction`, `complete`, `cancelled`, `unsuccessful`). У поточній реалізації вибірки залишаємо тільки `active` та `active.*`.
- **оголошення**: у контексті цього проекту = **тендер** (сутність API `/tenders`) або **аукціон** (сутність API `/auctions`). "Список оголошень" = результат list endpoint, "деталі оголошення" = результат `GET /tenders/{id}` або `GET /auctions/{id}`.
- **тендер**: процедура закупівлі через ProZorro API (`/tenders` endpoint).
- **аукціон**: процедура продажу майна/активів через ProZorro.Sale API (`/auctions` endpoint). Використовується для продажу нерухомості та інших активів.

