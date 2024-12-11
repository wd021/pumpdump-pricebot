# price_tracker.py
import os
import asyncio
import logging
from datetime import datetime, timezone
import aiohttp
from dotenv import load_dotenv
from supabase import create_client, Client
from functools import partial

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
UPDATE_INTERVAL = 5  # seconds
RETRY_INTERVAL = 300  # 5 minutes in seconds

async def run_in_executor(func, *args):
    """Helper function to run blocking operations in executor"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(func, *args))

class PriceTracker:
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env file")
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.current_period = None
        self.current_asset = None
        self.session = None
        
    async def setup(self):
        self.session = aiohttp.ClientSession()
        
    async def cleanup(self):
        if self.session:
            await self.session.close()

    async def get_asset(self, asset_id):
        """Get asset information from t_crypto_assets"""
        try:
            def fetch_asset():
                return self.supabase.table('t_crypto_assets') \
                    .select('*') \
                    .eq('id', asset_id) \
                    .single() \
                    .execute()

            response = await run_in_executor(fetch_asset)
            return response.data
        except Exception as e:
            logger.error(f"Error fetching asset: {e}")
            return None

    async def get_active_period(self):
        """Get the current active period that includes the current timestamp"""
        now = datetime.now(timezone.utc)
        
        try:
            def fetch_period():
                return self.supabase.table('t_prediction_periods') \
                    .select('*') \
                    .filter('starts_at', 'lte', now.isoformat()) \
                    .filter('ends_at', 'gt', now.isoformat()) \
                    .filter('is_active', 'eq', True) \
                    .execute()

            response = await run_in_executor(fetch_period)
            
            if response.data:
                period = response.data[0]
                # Fetch associated asset information
                asset = await self.get_asset(period['asset_id'])
                if asset:
                    self.current_asset = asset
                    return period
            return None
            
        except Exception as e:
            logger.error(f"Error fetching active period: {e}")
            return None

    async def fetch_candle_data(self, api_ticker, start_timestamp, end_timestamp):
        """Fetch historical candle data from Coinbase API"""
        url = f"https://api.coinbase.com/api/v3/brokerage/market/products/{api_ticker}/candles"
        params = {
            'start': str(int(start_timestamp.timestamp())),
            'end': str(int(end_timestamp.timestamp())),
            'granularity': 'ONE_DAY',
            'limit': 1
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"Candle API returned status {response.status}")
                
                data = await response.json()
                if not data.get('candles'):
                    raise Exception("No candle data received")
                
                candle = data['candles'][0]
                return {
                    'high': candle['high'],
                    'low': candle['low']
                }
                
        except Exception as e:
            logger.error(f"Error fetching candle data: {e}")
            return None

    async def fetch_current_price(self, api_ticker):
        """Fetch current price from Coinbase API"""
        url = f"https://api.coinbase.com/api/v3/brokerage/market/products/{api_ticker}"
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Price API returned status {response.status}")
                
                data = await response.json()
                return float(data['price'])
                
        except Exception as e:
            logger.error(f"Error fetching current price: {e}")
            return None

    async def fetch_all_prices(self, period):
        """Fetch both current price and candle data in parallel"""
        if not self.current_asset:
            logger.error("No asset information available")
            return None

        api_ticker = self.current_asset['apiTicker']
        start_time = datetime.fromisoformat(period['starts_at'])
        end_time = datetime.fromisoformat(period['ends_at'])

        # Fetch both current price and candle data concurrently
        current_price_task = self.fetch_current_price(api_ticker)
        candle_data_task = self.fetch_candle_data(api_ticker, start_time, end_time)
        
        current_price, candle_data = await asyncio.gather(
            current_price_task,
            candle_data_task
        )

        if current_price is None or candle_data is None:
            return None

        return {
            'current_price': current_price,
            'current_high': candle_data['high'],
            'current_low': candle_data['low']
        }

    async def update_period_prices(self, period, prices):
        """Update period with new prices"""
        try:
            def update_prices():
                return self.supabase.table('t_prediction_periods') \
                    .update({
                        'current_price': prices['current_price'],
                        'current_high': prices['current_high'],
                        'current_low': prices['current_low'],
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    }) \
                    .eq('id', period['id']) \
                    .execute()

            await run_in_executor(update_prices)
            logger.info(f"Updated prices for period {period['id']}: {prices}")
            
        except Exception as e:
            logger.error(f"Error updating prices: {e}")

    async def run(self):
        """Main run loop"""
        await self.setup()
        
        try:
            while True:
                # If we don't have a current period or the current period is expired
                if (not self.current_period or 
                    datetime.now(timezone.utc) >= datetime.fromisoformat(self.current_period['ends_at'])):
                    
                    self.current_period = await self.get_active_period()
                    
                    if not self.current_period:
                        logger.info("No active period found. Waiting before retry...")
                        await asyncio.sleep(RETRY_INTERVAL)
                        continue
                    
                    logger.info(f"Found new active period {self.current_period['id']} for asset {self.current_asset['apiTicker']}")

                # Fetch and update prices
                prices = await self.fetch_all_prices(self.current_period)
                if prices:
                    await self.update_period_prices(self.current_period, prices)

                await asyncio.sleep(UPDATE_INTERVAL)

        except Exception as e:
            logger.error(f"Fatal error in run loop: {e}")
        finally:
            await self.cleanup()

async def main():
    tracker = PriceTracker()
    try:
        await tracker.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await tracker.cleanup()

if __name__ == "__main__":
    asyncio.run(main())