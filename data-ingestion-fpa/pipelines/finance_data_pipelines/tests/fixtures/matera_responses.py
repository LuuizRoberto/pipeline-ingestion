"""
Fixtures de resposta da API Matera para uso nos testes.
Evita chamadas reais à API durante o CI.
"""

ACCOUNTING_FACTS_PAGE_1 = {
    "data": {
        "items": [
            {
                "companycode": "1",
                "accountingtypeCode": "A",
                "releasenumber": 1001,
                "releasetype": "C",
                "ledgeraccountcode": "1001",
                "accountingreleasedate": "2024-01-15",
                "amount": 1500.00,
            }
        ],
        "pagination": {
            "page": 0,
            "pageSize": 1000,
            "last": False,
        },
    }
}

ACCOUNTING_FACTS_PAGE_2 = {
    "data": {
        "items": [
            {
                "companycode": "1",
                "accountingtypeCode": "B",
                "releasenumber": 1002,
                "releasetype": "D",
                "ledgeraccountcode": "1002",
                "accountingreleasedate": "2024-01-16",
                "amount": 800.00,
            }
        ],
        "pagination": {
            "page": 1,
            "pageSize": 1000,
            "last": True,
        },
    }
}

LANCAMENTO_CONTABIL_RESPONSE = {
    "data": [
        {
            "dtlanccontabil": "2024-01-15",
            "dtgeracao": "2024-01-15",
            "numlanc": "1001",
            "codcontacontabil": "1001",
            "idcentrocusto": "CC01",
            "idlanccontacontabil": "LC001",
            "numguia": 1,
            "valor": 1500.00,
        }
    ]
}

OAUTH_TOKEN_RESPONSE = {
    "access_token": "fake-access-token-for-tests",
    "token_type": "Bearer",
    "expires_in": 3600,
}
