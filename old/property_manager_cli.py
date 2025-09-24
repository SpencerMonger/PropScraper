#!/usr/bin/env python3
"""
Property Data Manager CLI

Command-line interface for the enhanced property data management system
with dual-table architecture and change detection.

Usage:
    python property_manager_cli.py scrape --url "https://example.com/properties"
    python property_manager_cli.py sync --session-id "uuid-here"
    python property_manager_cli.py status
    python property_manager_cli.py quality-report --session-id "uuid-here"
"""

import asyncio
import argparse
import logging
import json
import os
from datetime import datetime
from typing import Dict, Optional
from supabase import create_client, Client

from services import PropertySyncOrchestrator, DataQualityService
from enhanced_property_scraper import EnhancedPropertyScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('property_manager.log')
    ]
)

logger = logging.getLogger(__name__)


class PropertyManagerCLI:
    """Command-line interface for property data management"""
    
    def __init__(self):
        self.supabase_client = self._init_supabase_client()
        self.orchestrator = PropertySyncOrchestrator(self.supabase_client)
        self.quality_service = DataQualityService(self.supabase_client)
        
    def _init_supabase_client(self) -> Client:
        """Initialize Supabase client from environment variables"""
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_ANON_KEY environment variables are required"
            )
        
        return create_client(supabase_url, supabase_key)

    async def scrape_properties(self, url: str, max_pages: int = 10, 
                              auto_sync: bool = True, session_name: Optional[str] = None) -> Dict:
        """Scrape properties from URL"""
        logger.info(f"Starting property scraping from: {url}")
        
        config = {
            'max_pages': max_pages,
            'delay_between_pages': 2
        }
        
        scraper = EnhancedPropertyScraper(self.supabase_client, config)
        
        result = await scraper.scrape_and_sync(
            target_url=url,
            session_name=session_name,
            auto_sync=auto_sync
        )
        
        return result

    async def sync_session(self, session_id: str, validate_data: bool = True) -> Dict:
        """Sync a specific session"""
        logger.info(f"Starting sync for session: {session_id}")
        
        workflow_result = await self.orchestrator.daily_sync_workflow(
            session_id, 
            config={'validate_data': validate_data}
        )
        
        return {
            'success': workflow_result.success,
            'session_id': workflow_result.session_id,
            'error': workflow_result.error_message,
            'execution_time_ms': workflow_result.execution_time_ms,
            'metrics': workflow_result.sync_result.metrics.__dict__ if workflow_result.sync_result else None
        }

    async def sync_pending_sessions(self, max_sessions: int = 10) -> Dict:
        """Sync all pending sessions"""
        logger.info("Finding and syncing pending sessions...")
        
        pending_sessions = await self.orchestrator.get_pending_sessions()
        
        if not pending_sessions:
            return {'message': 'No pending sessions found', 'synced': 0}
        
        # Limit number of sessions to process
        sessions_to_sync = pending_sessions[:max_sessions]
        
        results = await self.orchestrator.batch_sync_workflow(sessions_to_sync)
        
        successful = len([r for r in results if r.success])
        failed = len(results) - successful
        
        return {
            'total_pending': len(pending_sessions),
            'processed': len(results),
            'successful': successful,
            'failed': failed,
            'results': [
                {
                    'session_id': r.session_id,
                    'success': r.success,
                    'error': r.error_message
                } for r in results
            ]
        }

    async def get_system_status(self) -> Dict:
        """Get overall system status"""
        logger.info("Getting system status...")
        
        try:
            dashboard_data = await self.orchestrator.get_sync_dashboard_data(7)
            
            # Get additional statistics
            stats = {}
            
            # Total properties
            total_response = self.supabase_client.table('properties_live').select('id', count='exact').eq('status', 'active').execute()
            stats['total_active_properties'] = total_response.count
            
            # Staging data count
            staging_response = self.supabase_client.table('property_scrapes_staging').select('id', count='exact').execute()
            stats['staging_properties'] = staging_response.count
            
            # Recent sessions
            recent_sessions_response = self.supabase_client.table('scraping_sessions').select('*').order('created_at', desc=True).limit(5).execute()
            stats['recent_sessions'] = recent_sessions_response.data
            
            return {
                'status': 'operational',
                'timestamp': datetime.utcnow().isoformat(),
                'statistics': stats,
                'dashboard_data': dashboard_data
            }
            
        except Exception as e:
            logger.error(f"Error getting system status: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

    async def generate_quality_report(self, session_id: str) -> Dict:
        """Generate quality report for a session"""
        logger.info(f"Generating quality report for session: {session_id}")
        
        try:
            report = await self.quality_service.generate_quality_report(session_id)
            return report
            
        except Exception as e:
            logger.error(f"Error generating quality report: {str(e)}")
            return {'error': str(e)}

    async def run_quality_checks(self, session_id: str) -> Dict:
        """Run comprehensive quality checks"""
        logger.info(f"Running quality checks for session: {session_id}")
        
        try:
            checks = await self.quality_service.run_quality_checks(session_id)
            return checks
            
        except Exception as e:
            logger.error(f"Error running quality checks: {str(e)}")
            return {'error': str(e)}

    async def cleanup_old_data(self, days_to_keep: int = 30) -> Dict:
        """Clean up old data"""
        logger.info(f"Cleaning up data older than {days_to_keep} days...")
        
        try:
            stats = await self.orchestrator.cleanup_old_data(days_to_keep)
            return stats
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            return {'error': str(e)}

    async def list_sessions(self, status: Optional[str] = None, limit: int = 20) -> Dict:
        """List scraping sessions"""
        try:
            query = self.supabase_client.table('scraping_sessions').select('*').order('created_at', desc=True).limit(limit)
            
            if status:
                query = query.eq('status', status)
            
            response = query.execute()
            
            return {
                'sessions': response.data,
                'count': len(response.data)
            }
            
        except Exception as e:
            logger.error(f"Error listing sessions: {str(e)}")
            return {'error': str(e)}

    async def get_session_details(self, session_id: str) -> Dict:
        """Get detailed information about a session"""
        try:
            # Get session info
            session_response = self.supabase_client.table('scraping_sessions').select('*').eq('id', session_id).execute()
            
            if not session_response.data:
                return {'error': 'Session not found'}
            
            session_info = session_response.data[0]
            
            # Get staging data count
            staging_response = self.supabase_client.table('property_scrapes_staging').select('id', count='exact').eq('session_id', session_id).execute()
            
            # Get sync metadata
            sync_response = self.supabase_client.table('sync_metadata').select('*').eq('session_id', session_id).execute()
            
            # Get errors
            errors_response = self.supabase_client.table('scraping_errors').select('*').eq('session_id', session_id).execute()
            
            return {
                'session': session_info,
                'staging_count': staging_response.count,
                'sync_metadata': sync_response.data[0] if sync_response.data else None,
                'errors': errors_response.data,
                'error_count': len(errors_response.data)
            }
            
        except Exception as e:
            logger.error(f"Error getting session details: {str(e)}")
            return {'error': str(e)}


def print_json(data: Dict):
    """Pretty print JSON data"""
    print(json.dumps(data, indent=2, default=str))


async def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='Property Data Manager CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Scrape command
    scrape_parser = subparsers.add_parser('scrape', help='Scrape properties from URL')
    scrape_parser.add_argument('--url', required=True, help='URL to scrape')
    scrape_parser.add_argument('--max-pages', type=int, default=10, help='Maximum pages to scrape')
    scrape_parser.add_argument('--no-auto-sync', action='store_true', help='Disable automatic sync')
    scrape_parser.add_argument('--session-name', help='Optional session name')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Sync session data')
    sync_parser.add_argument('--session-id', required=True, help='Session ID to sync')
    sync_parser.add_argument('--no-validation', action='store_true', help='Skip data validation')
    
    # Sync pending command
    sync_pending_parser = subparsers.add_parser('sync-pending', help='Sync all pending sessions')
    sync_pending_parser.add_argument('--max-sessions', type=int, default=10, help='Maximum sessions to sync')
    
    # Status command
    subparsers.add_parser('status', help='Get system status')
    
    # Quality report command
    quality_parser = subparsers.add_parser('quality-report', help='Generate quality report')
    quality_parser.add_argument('--session-id', required=True, help='Session ID for quality report')
    
    # Quality checks command
    checks_parser = subparsers.add_parser('quality-checks', help='Run quality checks')
    checks_parser.add_argument('--session-id', required=True, help='Session ID for quality checks')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old data')
    cleanup_parser.add_argument('--days', type=int, default=30, help='Days of data to keep')
    
    # List sessions command
    list_parser = subparsers.add_parser('list-sessions', help='List scraping sessions')
    list_parser.add_argument('--status', help='Filter by status')
    list_parser.add_argument('--limit', type=int, default=20, help='Number of sessions to return')
    
    # Session details command
    details_parser = subparsers.add_parser('session-details', help='Get session details')
    details_parser.add_argument('--session-id', required=True, help='Session ID')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        cli = PropertyManagerCLI()
        
        if args.command == 'scrape':
            result = await cli.scrape_properties(
                url=args.url,
                max_pages=args.max_pages,
                auto_sync=not args.no_auto_sync,
                session_name=args.session_name
            )
            print_json(result)
            
        elif args.command == 'sync':
            result = await cli.sync_session(
                session_id=args.session_id,
                validate_data=not args.no_validation
            )
            print_json(result)
            
        elif args.command == 'sync-pending':
            result = await cli.sync_pending_sessions(args.max_sessions)
            print_json(result)
            
        elif args.command == 'status':
            result = await cli.get_system_status()
            print_json(result)
            
        elif args.command == 'quality-report':
            result = await cli.generate_quality_report(args.session_id)
            print_json(result)
            
        elif args.command == 'quality-checks':
            result = await cli.run_quality_checks(args.session_id)
            print_json(result)
            
        elif args.command == 'cleanup':
            result = await cli.cleanup_old_data(args.days)
            print_json(result)
            
        elif args.command == 'list-sessions':
            result = await cli.list_sessions(args.status, args.limit)
            print_json(result)
            
        elif args.command == 'session-details':
            result = await cli.get_session_details(args.session_id)
            print_json(result)
            
    except Exception as e:
        logger.error(f"CLI error: {str(e)}")
        print_json({'error': str(e)})


if __name__ == '__main__':
    asyncio.run(main()) 