import requests
import time
from datetime import datetime
from typing import Optional

class CurrencyConverter:
    """
    Utilitário para conversão de moedas usando dados do Banco Central do Brasil (PTAX).
    """
    
    # API do Banco Central - Série 10813 (Dólar Comercial Venda - PTAX)
    BCB_API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados/ultimos/1?formato=json"
    
    _rate: float = 5.50  # Fallback inicial seguro
    _last_update: float = 0
    _cache_ttl: int = 3600  # 1 hora de cache

    @classmethod
    def get_usd_brl_rate(cls) -> float:
        """
        Obtém a cotação atual do Dólar (PTAX Venda).
        Usa cache em memória para evitar chamadas excessivas.
        """
        now = time.time()
        
        # Retorna cache se ainda válido
        if (now - cls._last_update) < cls._cache_ttl:
            return cls._rate

        try:
            # Timeout curto para não travar o monitoramento
            response = requests.get(cls.BCB_API_URL, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, list) and 'valor' in data[0]:
                    # O valor vem como string "5.1234"
                    cls._rate = float(data[0]['valor'])
                    cls._last_update = now
                    print(f"   💵 Cotação PTAX atualizada: R$ {cls._rate:.4f} (Data: {data[0].get('data')})")
                    return cls._rate
            
            print(f"   ⚠️  Erro ao consultar BCB (Status {response.status_code}). Usando taxa anterior: {cls._rate}")
            
        except Exception as e:
            print(f"   ⚠️  Falha na conexão com Banco Central: {e}. Usando taxa fallback: {cls._rate}")
            
        return cls._rate
