"""Unit tests for authentication."""

import os
import pytest
from unittest.mock import patch, MagicMock
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ProfileNotFound
from athena_query_tool.auth import AuthenticationManager
from athena_query_tool.exceptions import AuthenticationError


class TestAuthenticationManager:
    """Test cases for AuthenticationManager."""
    
    def test_get_session_with_environment_variables(self):
        """Test authentication using environment variables."""
        auth_manager = AuthenticationManager()
        
        with patch.dict(os.environ, {
            'AWS_ACCESS_KEY_ID': 'test_access_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret_key',
            'AWS_DEFAULT_REGION': 'us-east-1'
        }):
            session = auth_manager.get_session()
            
            assert session is not None
            assert isinstance(session, boto3.Session)
            credentials = session.get_credentials()
            assert credentials is not None
            assert credentials.access_key == 'test_access_key'
            assert credentials.secret_key == 'test_secret_key'
    
    def test_get_session_with_profile(self):
        """Test authentication using AWS profile."""
        auth_manager = AuthenticationManager()
        
        # Mock boto3.Session to avoid requiring actual AWS credentials
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_credentials.access_key = 'profile_access_key'
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            session = auth_manager.get_session(profile='test-profile', region='us-west-2')
            
            # Verify Session was created with correct parameters
            mock_session_class.assert_called_once_with(
                profile_name='test-profile',
                region_name='us-west-2'
            )
            assert session is not None
    
    def test_get_session_with_region(self):
        """Test session creation with specified region."""
        auth_manager = AuthenticationManager()
        
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            session = auth_manager.get_session(region='eu-west-1')
            
            mock_session_class.assert_called_once_with(
                profile_name=None,
                region_name='eu-west-1'
            )
            assert session is not None
    
    def test_get_session_no_credentials_error(self):
        """Test error handling when no credentials are found."""
        auth_manager = AuthenticationManager()
        
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_session.get_credentials.return_value = None
            mock_session_class.return_value = mock_session
            
            with pytest.raises(AuthenticationError) as exc_info:
                auth_manager.get_session()
            
            assert "No valid AWS credentials found" in str(exc_info.value)
            assert "Environment variables" in str(exc_info.value)
            assert "AWS credentials file" in str(exc_info.value)
    
    def test_get_session_profile_not_found(self):
        """Test error handling when specified profile doesn't exist."""
        auth_manager = AuthenticationManager()
        
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session_class.side_effect = ProfileNotFound(profile='nonexistent')
            
            with pytest.raises(AuthenticationError) as exc_info:
                auth_manager.get_session(profile='nonexistent')
            
            assert "profile 'nonexistent' not found" in str(exc_info.value).lower()
            assert "AWS configuration file" in str(exc_info.value)
    
    def test_get_session_partial_credentials_error(self):
        """Test error handling when credentials are incomplete."""
        auth_manager = AuthenticationManager()
        
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session_class.side_effect = PartialCredentialsError(
                provider='env',
                cred_var='AWS_SECRET_ACCESS_KEY'
            )
            
            with pytest.raises(AuthenticationError) as exc_info:
                auth_manager.get_session()
            
            assert "Incomplete AWS credentials" in str(exc_info.value)
            assert "AWS_ACCESS_KEY_ID" in str(exc_info.value)
            assert "AWS_SECRET_ACCESS_KEY" in str(exc_info.value)
    
    def test_get_session_generic_error(self):
        """Test error handling for unexpected errors."""
        auth_manager = AuthenticationManager()
        
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session_class.side_effect = Exception("Unexpected error")
            
            with pytest.raises(AuthenticationError) as exc_info:
                auth_manager.get_session()
            
            assert "Failed to create AWS session" in str(exc_info.value)
            assert "Unexpected error" in str(exc_info.value)
    
    def test_get_session_without_parameters(self):
        """Test session creation without profile or region parameters."""
        auth_manager = AuthenticationManager()
        
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            session = auth_manager.get_session()
            
            mock_session_class.assert_called_once_with(
                profile_name=None,
                region_name=None
            )
            assert session is not None
    
    def test_get_session_with_credentials_file(self):
        """Test authentication using AWS credentials file (default profile)."""
        auth_manager = AuthenticationManager()
        
        # Mock boto3.Session to simulate credentials file authentication
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_credentials.access_key = 'file_access_key'
            mock_credentials.secret_key = 'file_secret_key'
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            # Clear environment variables to ensure we're testing file-based auth
            with patch.dict(os.environ, {}, clear=True):
                session = auth_manager.get_session()
                
                assert session is not None
                credentials = session.get_credentials()
                assert credentials is not None
                assert credentials.access_key == 'file_access_key'
    
    def test_get_session_with_iam_role(self):
        """Test authentication using IAM role (simulated)."""
        auth_manager = AuthenticationManager()
        
        # Mock boto3.Session to simulate IAM role authentication
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_credentials.access_key = 'role_access_key'
            mock_credentials.secret_key = 'role_secret_key'
            mock_credentials.token = 'role_session_token'
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            session = auth_manager.get_session()
            
            assert session is not None
            credentials = session.get_credentials()
            assert credentials is not None
            assert credentials.access_key == 'role_access_key'
            assert credentials.token == 'role_session_token'
    
    def test_credential_provider_chain_order(self):
        """Test that credential provider chain is followed in correct order.
        
        This test validates that boto3.Session is called with the correct
        parameters, which allows boto3 to follow its standard credential
        provider chain:
        1. Environment variables
        2. Profile (if specified)
        3. Credentials file
        4. IAM role
        """
        auth_manager = AuthenticationManager()
        
        # Test 1: With profile specified, it should be passed to Session
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            auth_manager.get_session(profile='my-profile')
            
            # Verify profile is passed, allowing boto3 to prioritize it
            mock_session_class.assert_called_once_with(
                profile_name='my-profile',
                region_name=None
            )
        
        # Test 2: Without profile, boto3 follows default chain
        with patch('athena_query_tool.auth.boto3.Session') as mock_session_class:
            mock_session = MagicMock()
            mock_credentials = MagicMock()
            mock_session.get_credentials.return_value = mock_credentials
            mock_session_class.return_value = mock_session
            
            auth_manager.get_session()
            
            # Verify no profile is passed, allowing boto3 to use default chain
            mock_session_class.assert_called_once_with(
                profile_name=None,
                region_name=None
            )
