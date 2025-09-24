"""
Data Quality Service for Property Data Management

This service implements data validation, quality scoring, and monitoring
based on configurable rules and business logic.

Based on property_data_management_architecture.md
"""

import asyncio
import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dataclasses import dataclass
from supabase import Client
import json

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Represents a data validation error"""
    property_id: str
    field_name: str
    rule_name: str
    error_message: str
    severity: str
    current_value: Any = None


@dataclass
class ValidationResult:
    """Results from data validation process"""
    total_properties: int
    valid_properties: int
    invalid_properties: int
    errors: List[ValidationError]
    overall_score: float
    completeness_rate: float
    field_scores: Dict[str, float]


class DataQualityService:
    """
    Service for validating data quality and calculating quality scores
    """
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
        self.validation_rules = {}
        self.quality_thresholds = {
            'completeness_rate': 0.85,
            'duplicate_rate': 0.02,
            'error_rate': 0.05,
            'freshness_hours': 24
        }
        
    async def load_validation_rules(self):
        """Load validation rules from database"""
        try:
            response = self.supabase.table('validation_rules').select('*').eq('is_active', True).execute()
            
            self.validation_rules = {}
            for rule in response.data:
                field_name = rule['field_name']
                if field_name not in self.validation_rules:
                    self.validation_rules[field_name] = []
                self.validation_rules[field_name].append(rule)
                
            logger.info(f"Loaded {len(response.data)} validation rules")
            
        except Exception as e:
            logger.error(f"Error loading validation rules: {str(e)}")
            # Use default rules if database load fails
            await self._load_default_rules()

    async def _load_default_rules(self):
        """Load default validation rules if database rules are unavailable"""
        self.validation_rules = {
            'title': [
                {
                    'rule_name': 'required_title',
                    'rule_type': 'required',
                    'rule_config': {},
                    'severity': 'error'
                }
            ],
            'price': [
                {
                    'rule_name': 'price_range',
                    'rule_type': 'range',
                    'rule_config': {'min': 1000, 'max': 50000000},
                    'severity': 'warning'
                }
            ],
            'bedrooms': [
                {
                    'rule_name': 'bedrooms_range',
                    'rule_type': 'range',
                    'rule_config': {'min': 0, 'max': 20},
                    'severity': 'warning'
                }
            ],
            'bathrooms': [
                {
                    'rule_name': 'bathrooms_range',
                    'rule_type': 'range',
                    'rule_config': {'min': 0, 'max': 10},
                    'severity': 'warning'
                }
            ],
            'agent_email': [
                {
                    'rule_name': 'valid_email',
                    'rule_type': 'pattern',
                    'rule_config': {'regex': r'^[^@]+@[^@]+\.[^@]+$'},
                    'severity': 'warning'
                }
            ],
            'latitude': [
                {
                    'rule_name': 'valid_latitude',
                    'rule_type': 'range',
                    'rule_config': {'min': -90, 'max': 90},
                    'severity': 'warning'
                }
            ],
            'longitude': [
                {
                    'rule_name': 'valid_longitude',
                    'rule_type': 'range',
                    'rule_config': {'min': -180, 'max': 180},
                    'severity': 'warning'
                }
            ]
        }

    async def validate_staging_data(self, session_id: str) -> ValidationResult:
        """
        Validate all staging data for a session
        
        Args:
            session_id: The scraping session ID to validate
            
        Returns:
            ValidationResult with detailed validation information
        """
        try:
            logger.info(f"Starting data validation for session {session_id}")
            
            # Load validation rules
            await self.load_validation_rules()
            
            # Get staging data
            staging_response = self.supabase.table('property_scrapes_staging').select('*').eq('session_id', session_id).execute()
            staging_data = staging_response.data
            
            if not staging_data:
                logger.warning(f"No staging data found for session {session_id}")
                return ValidationResult(0, 0, 0, [], 1.0, 1.0, {})
            
            # Validate each property
            all_errors = []
            valid_count = 0
            field_scores = {}
            
            for property_data in staging_data:
                property_errors = await self._validate_property(property_data)
                all_errors.extend(property_errors)
                
                if not any(error.severity == 'error' for error in property_errors):
                    valid_count += 1
            
            # Calculate field-specific scores
            field_scores = await self._calculate_field_scores(staging_data, all_errors)
            
            # Calculate overall metrics
            total_properties = len(staging_data)
            invalid_properties = total_properties - valid_count
            overall_score = await self._calculate_overall_score(staging_data, all_errors)
            completeness_rate = await self._calculate_completeness_rate(staging_data)
            
            result = ValidationResult(
                total_properties=total_properties,
                valid_properties=valid_count,
                invalid_properties=invalid_properties,
                errors=all_errors,
                overall_score=overall_score,
                completeness_rate=completeness_rate,
                field_scores=field_scores
            )
            
            logger.info(f"Validation completed: {valid_count}/{total_properties} valid properties, "
                       f"overall score: {overall_score:.2f}, completeness: {completeness_rate:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error during data validation: {str(e)}")
            raise

    async def _validate_property(self, property_data: Dict) -> List[ValidationError]:
        """
        Validate a single property against all applicable rules
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            List of ValidationError objects
        """
        errors = []
        property_id = property_data.get('property_id', 'unknown')
        
        # Check each field that has validation rules
        for field_name, rules in self.validation_rules.items():
            field_value = property_data.get(field_name)
            
            for rule in rules:
                error = await self._apply_validation_rule(
                    property_id, field_name, field_value, rule
                )
                if error:
                    errors.append(error)
        
        # Additional custom validations
        custom_errors = await self._apply_custom_validations(property_data)
        errors.extend(custom_errors)
        
        return errors

    async def _apply_validation_rule(self, property_id: str, field_name: str, 
                                   field_value: Any, rule: Dict) -> Optional[ValidationError]:
        """
        Apply a single validation rule to a field value
        
        Args:
            property_id: Property ID
            field_name: Field name
            field_value: Field value to validate
            rule: Validation rule dictionary
            
        Returns:
            ValidationError if validation fails, None otherwise
        """
        rule_type = rule['rule_type']
        rule_config = rule['rule_config']
        rule_name = rule['rule_name']
        severity = rule['severity']
        
        try:
            if rule_type == 'required':
                if field_value is None or str(field_value).strip() == '':
                    return ValidationError(
                        property_id=property_id,
                        field_name=field_name,
                        rule_name=rule_name,
                        error_message=f"Required field '{field_name}' is missing or empty",
                        severity=severity,
                        current_value=field_value
                    )
                    
            elif rule_type == 'range':
                if field_value is not None:
                    try:
                        numeric_value = float(field_value)
                        min_val = rule_config.get('min')
                        max_val = rule_config.get('max')
                        
                        if min_val is not None and numeric_value < min_val:
                            return ValidationError(
                                property_id=property_id,
                                field_name=field_name,
                                rule_name=rule_name,
                                error_message=f"Value {numeric_value} is below minimum {min_val}",
                                severity=severity,
                                current_value=field_value
                            )
                            
                        if max_val is not None and numeric_value > max_val:
                            return ValidationError(
                                property_id=property_id,
                                field_name=field_name,
                                rule_name=rule_name,
                                error_message=f"Value {numeric_value} is above maximum {max_val}",
                                severity=severity,
                                current_value=field_value
                            )
                    except (ValueError, TypeError):
                        return ValidationError(
                            property_id=property_id,
                            field_name=field_name,
                            rule_name=rule_name,
                            error_message=f"Value '{field_value}' is not a valid number",
                            severity=severity,
                            current_value=field_value
                        )
                        
            elif rule_type == 'pattern':
                if field_value is not None and str(field_value).strip():
                    pattern = rule_config.get('regex')
                    if pattern and not re.match(pattern, str(field_value)):
                        return ValidationError(
                            property_id=property_id,
                            field_name=field_name,
                            rule_name=rule_name,
                            error_message=f"Value '{field_value}' does not match required pattern",
                            severity=severity,
                            current_value=field_value
                        )
                        
            elif rule_type == 'custom':
                # Handle custom validation logic
                custom_error = await self._apply_custom_rule(
                    property_id, field_name, field_value, rule
                )
                if custom_error:
                    return custom_error
                    
        except Exception as e:
            logger.error(f"Error applying rule {rule_name}: {str(e)}")
            
        return None

    async def _apply_custom_validations(self, property_data: Dict) -> List[ValidationError]:
        """
        Apply custom business logic validations
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            List of ValidationError objects
        """
        errors = []
        property_id = property_data.get('property_id', 'unknown')
        
        # Validate coordinate consistency
        lat = property_data.get('latitude')
        lng = property_data.get('longitude')
        gps_coords = property_data.get('gps_coordinates')
        
        if lat is not None and lng is not None and gps_coords:
            # Check if GPS coordinates string matches lat/lng values
            try:
                coords_parts = gps_coords.split(',')
                if len(coords_parts) == 2:
                    gps_lat = float(coords_parts[0].strip())
                    gps_lng = float(coords_parts[1].strip())
                    
                    if abs(float(lat) - gps_lat) > 0.001 or abs(float(lng) - gps_lng) > 0.001:
                        errors.append(ValidationError(
                            property_id=property_id,
                            field_name='gps_coordinates',
                            rule_name='coordinate_consistency',
                            error_message="GPS coordinates string doesn't match latitude/longitude values",
                            severity='warning',
                            current_value=gps_coords
                        ))
            except (ValueError, IndexError):
                errors.append(ValidationError(
                    property_id=property_id,
                    field_name='gps_coordinates',
                    rule_name='coordinate_format',
                    error_message="GPS coordinates format is invalid",
                    severity='warning',
                    current_value=gps_coords
                ))
        
        # Validate price consistency
        price = property_data.get('price')
        price_per_m2 = property_data.get('price_per_m2')
        total_area = property_data.get('total_area_m2')
        
        if price and price_per_m2 and total_area:
            try:
                calculated_price = float(price_per_m2) * float(total_area)
                price_diff_percent = abs(float(price) - calculated_price) / float(price)
                
                if price_diff_percent > 0.2:  # 20% tolerance
                    errors.append(ValidationError(
                        property_id=property_id,
                        field_name='price_per_m2',
                        rule_name='price_consistency',
                        error_message=f"Price per m2 calculation inconsistent with total price (diff: {price_diff_percent:.1%})",
                        severity='info',
                        current_value=price_per_m2
                    ))
            except (ValueError, ZeroDivisionError):
                pass
        
        # Validate image URLs
        image_urls = property_data.get('image_urls')
        if image_urls and isinstance(image_urls, list):
            for i, url in enumerate(image_urls):
                if url and not self._is_valid_url(url):
                    errors.append(ValidationError(
                        property_id=property_id,
                        field_name='image_urls',
                        rule_name='valid_image_url',
                        error_message=f"Invalid image URL at index {i}: {url}",
                        severity='warning',
                        current_value=url
                    ))
        
        return errors

    def _is_valid_url(self, url: str) -> bool:
        """Check if a string is a valid URL"""
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return url_pattern.match(url) is not None

    async def _apply_custom_rule(self, property_id: str, field_name: str, 
                               field_value: Any, rule: Dict) -> Optional[ValidationError]:
        """
        Apply custom validation rule logic
        
        Args:
            property_id: Property ID
            field_name: Field name
            field_value: Field value
            rule: Custom rule dictionary
            
        Returns:
            ValidationError if validation fails, None otherwise
        """
        # Implement custom rule logic based on rule_config
        # This can be extended for specific business rules
        return None

    async def _calculate_field_scores(self, staging_data: List[Dict], 
                                    errors: List[ValidationError]) -> Dict[str, float]:
        """
        Calculate quality scores for individual fields
        
        Args:
            staging_data: List of property data
            errors: List of validation errors
            
        Returns:
            Dictionary mapping field names to quality scores
        """
        field_scores = {}
        total_properties = len(staging_data)
        
        if total_properties == 0:
            return field_scores
        
        # Get all fields that appear in the data
        all_fields = set()
        for property_data in staging_data:
            all_fields.update(property_data.keys())
        
        # Calculate score for each field
        for field_name in all_fields:
            # Count properties with this field populated
            populated_count = sum(1 for prop in staging_data 
                                if prop.get(field_name) is not None and str(prop.get(field_name)).strip())
            
            # Count errors for this field
            field_errors = [e for e in errors if e.field_name == field_name and e.severity == 'error']
            error_count = len(field_errors)
            
            # Calculate score: completeness - error rate
            completeness = populated_count / total_properties
            error_rate = error_count / total_properties
            score = max(0.0, completeness - error_rate)
            
            field_scores[field_name] = score
        
        return field_scores

    async def _calculate_overall_score(self, staging_data: List[Dict], 
                                     errors: List[ValidationError]) -> float:
        """
        Calculate overall data quality score
        
        Args:
            staging_data: List of property data
            errors: List of validation errors
            
        Returns:
            Overall quality score between 0.0 and 1.0
        """
        if not staging_data:
            return 1.0
        
        total_properties = len(staging_data)
        
        # Count critical errors (severity = 'error')
        critical_errors = [e for e in errors if e.severity == 'error']
        critical_error_rate = len(critical_errors) / total_properties
        
        # Count warnings
        warnings = [e for e in errors if e.severity == 'warning']
        warning_rate = len(warnings) / total_properties
        
        # Calculate completeness rate
        completeness_rate = await self._calculate_completeness_rate(staging_data)
        
        # Weighted score calculation
        # Completeness: 40%, Critical errors: -40%, Warnings: -20%
        score = (completeness_rate * 0.4) + \
                (max(0, 1.0 - critical_error_rate) * 0.4) + \
                (max(0, 1.0 - warning_rate * 0.5) * 0.2)
        
        return min(1.0, max(0.0, score))

    async def _calculate_completeness_rate(self, staging_data: List[Dict]) -> float:
        """
        Calculate data completeness rate
        
        Args:
            staging_data: List of property data
            
        Returns:
            Completeness rate between 0.0 and 1.0
        """
        if not staging_data:
            return 1.0
        
        # Define critical fields and their weights
        critical_fields = {
            'title': 0.2,
            'price': 0.2,
            'property_type': 0.1,
            'operation_type': 0.1,
            'city': 0.1,
            'address': 0.1,
            'bedrooms': 0.05,
            'bathrooms': 0.05,
            'main_image_url': 0.1
        }
        
        total_score = 0.0
        
        for property_data in staging_data:
            property_score = 0.0
            
            for field, weight in critical_fields.items():
                value = property_data.get(field)
                if value is not None and str(value).strip():
                    property_score += weight
            
            total_score += property_score
        
        return total_score / len(staging_data)

    async def run_quality_checks(self, session_id: str) -> Dict:
        """
        Run comprehensive data quality checks
        
        Args:
            session_id: Session ID to check
            
        Returns:
            Dictionary with quality check results
        """
        try:
            logger.info(f"Running quality checks for session {session_id}")
            
            results = {}
            
            # Basic validation
            validation_result = await self.validate_staging_data(session_id)
            results['validation'] = {
                'total_properties': validation_result.total_properties,
                'valid_properties': validation_result.valid_properties,
                'invalid_properties': validation_result.invalid_properties,
                'overall_score': validation_result.overall_score,
                'completeness_rate': validation_result.completeness_rate,
                'error_count': len(validation_result.errors),
                'critical_errors': len([e for e in validation_result.errors if e.severity == 'error'])
            }
            
            # Duplicate check
            duplicate_rate = await self._check_duplicates(session_id)
            results['duplicates'] = {
                'duplicate_rate': duplicate_rate,
                'passes_threshold': duplicate_rate <= self.quality_thresholds['duplicate_rate']
            }
            
            # Data freshness check
            freshness_score = await self._check_data_freshness(session_id)
            results['freshness'] = {
                'freshness_score': freshness_score,
                'passes_threshold': freshness_score >= 0.8
            }
            
            # Geographic distribution check
            geo_distribution = await self._check_geographic_distribution(session_id)
            results['geographic_distribution'] = geo_distribution
            
            # Overall quality assessment
            overall_quality = (
                validation_result.overall_score * 0.4 +
                (1.0 - duplicate_rate) * 0.2 +
                freshness_score * 0.2 +
                geo_distribution.get('diversity_score', 0.8) * 0.2
            )
            
            results['overall_quality'] = overall_quality
            results['passes_all_thresholds'] = (
                validation_result.overall_score >= 0.7 and
                duplicate_rate <= self.quality_thresholds['duplicate_rate'] and
                freshness_score >= 0.8
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error running quality checks: {str(e)}")
            return {'error': str(e)}

    async def _check_duplicates(self, session_id: str) -> float:
        """Check for duplicate properties in staging data"""
        try:
            response = self.supabase.table('property_scrapes_staging').select('property_id').eq('session_id', session_id).execute()
            
            property_ids = [item['property_id'] for item in response.data]
            total_count = len(property_ids)
            unique_count = len(set(property_ids))
            
            if total_count == 0:
                return 0.0
                
            duplicate_rate = (total_count - unique_count) / total_count
            return duplicate_rate
            
        except Exception as e:
            logger.error(f"Error checking duplicates: {str(e)}")
            return 0.0

    async def _check_data_freshness(self, session_id: str) -> float:
        """Check data freshness based on scraping timestamps"""
        try:
            response = self.supabase.table('property_scrapes_staging').select('scraped_at').eq('session_id', session_id).execute()
            
            if not response.data:
                return 0.0
            
            now = datetime.utcnow()
            fresh_count = 0
            
            for item in response.data:
                scraped_at = datetime.fromisoformat(item['scraped_at'].replace('Z', '+00:00'))
                hours_old = (now - scraped_at).total_seconds() / 3600
                
                if hours_old <= self.quality_thresholds['freshness_hours']:
                    fresh_count += 1
            
            return fresh_count / len(response.data)
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            return 0.0

    async def _check_geographic_distribution(self, session_id: str) -> Dict:
        """Check geographic distribution of properties"""
        try:
            response = self.supabase.table('property_scrapes_staging').select('city, neighborhood, latitude, longitude').eq('session_id', session_id).execute()
            
            if not response.data:
                return {'diversity_score': 0.0, 'city_count': 0, 'neighborhood_count': 0}
            
            cities = set()
            neighborhoods = set()
            coordinates = []
            
            for item in response.data:
                if item.get('city'):
                    cities.add(item['city'])
                if item.get('neighborhood'):
                    neighborhoods.add(item['neighborhood'])
                if item.get('latitude') and item.get('longitude'):
                    coordinates.append((float(item['latitude']), float(item['longitude'])))
            
            # Calculate diversity score based on geographic spread
            city_diversity = min(1.0, len(cities) / 10)  # Normalize to max 10 cities
            neighborhood_diversity = min(1.0, len(neighborhoods) / 50)  # Normalize to max 50 neighborhoods
            
            # Simple coordinate spread check
            coordinate_spread = 0.8  # Default score
            if len(coordinates) > 1:
                lats = [coord[0] for coord in coordinates]
                lngs = [coord[1] for coord in coordinates]
                lat_range = max(lats) - min(lats)
                lng_range = max(lngs) - min(lngs)
                
                # Higher spread = better diversity (up to a reasonable limit)
                coordinate_spread = min(1.0, (lat_range + lng_range) / 2.0)
            
            diversity_score = (city_diversity * 0.4 + neighborhood_diversity * 0.4 + coordinate_spread * 0.2)
            
            return {
                'diversity_score': diversity_score,
                'city_count': len(cities),
                'neighborhood_count': len(neighborhoods),
                'coordinate_count': len(coordinates)
            }
            
        except Exception as e:
            logger.error(f"Error checking geographic distribution: {str(e)}")
            return {'diversity_score': 0.0, 'error': str(e)}

    async def generate_quality_report(self, session_id: str) -> Dict:
        """
        Generate comprehensive quality report for a session
        
        Args:
            session_id: Session ID
            
        Returns:
            Dictionary with detailed quality report
        """
        try:
            validation_result = await self.validate_staging_data(session_id)
            quality_checks = await self.run_quality_checks(session_id)
            
            # Group errors by severity and field
            errors_by_severity = {}
            errors_by_field = {}
            
            for error in validation_result.errors:
                # By severity
                if error.severity not in errors_by_severity:
                    errors_by_severity[error.severity] = []
                errors_by_severity[error.severity].append(error)
                
                # By field
                if error.field_name not in errors_by_field:
                    errors_by_field[error.field_name] = []
                errors_by_field[error.field_name].append(error)
            
            report = {
                'session_id': session_id,
                'generated_at': datetime.utcnow().isoformat(),
                'summary': {
                    'total_properties': validation_result.total_properties,
                    'valid_properties': validation_result.valid_properties,
                    'invalid_properties': validation_result.invalid_properties,
                    'overall_score': validation_result.overall_score,
                    'completeness_rate': validation_result.completeness_rate
                },
                'validation_details': {
                    'errors_by_severity': {
                        severity: len(errors) for severity, errors in errors_by_severity.items()
                    },
                    'errors_by_field': {
                        field: len(errors) for field, errors in errors_by_field.items()
                    },
                    'field_scores': validation_result.field_scores
                },
                'quality_checks': quality_checks,
                'recommendations': await self._generate_recommendations(validation_result, quality_checks)
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating quality report: {str(e)}")
            return {'error': str(e)}

    async def _generate_recommendations(self, validation_result: ValidationResult, 
                                      quality_checks: Dict) -> List[str]:
        """Generate recommendations based on quality analysis"""
        recommendations = []
        
        # Validation recommendations
        if validation_result.overall_score < 0.7:
            recommendations.append("Overall data quality is below threshold. Review data extraction logic.")
        
        if validation_result.completeness_rate < 0.8:
            recommendations.append("Data completeness is low. Check if all required fields are being extracted.")
        
        # Critical errors
        critical_errors = [e for e in validation_result.errors if e.severity == 'error']
        if len(critical_errors) > validation_result.total_properties * 0.1:
            recommendations.append("High number of critical errors detected. Review data validation rules.")
        
        # Field-specific recommendations
        for field_name, score in validation_result.field_scores.items():
            if score < 0.5:
                recommendations.append(f"Field '{field_name}' has low quality score ({score:.2f}). Review extraction logic.")
        
        # Duplicate recommendations
        if quality_checks.get('duplicates', {}).get('duplicate_rate', 0) > 0.02:
            recommendations.append("High duplicate rate detected. Review property ID generation logic.")
        
        # Freshness recommendations
        if quality_checks.get('freshness', {}).get('freshness_score', 1) < 0.8:
            recommendations.append("Data freshness is low. Consider more frequent scraping schedules.")
        
        return recommendations 