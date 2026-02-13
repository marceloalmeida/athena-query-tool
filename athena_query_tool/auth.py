"""AWS authentication management."""

from typing import Optional
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ProfileNotFound
from .exceptions import AuthenticationError


class AuthenticationManager:
    """Manages AWS credential resolution and session creation."""
    
    def get_session(self, profile: Optional[str] = None, region: Optional[str] = None) -> boto3.Session:
        """
        Create boto3 session with resolved credentials.
        
        Follows the AWS credential provider chain:
        1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        2. Profile specified in configuration
        3. AWS credentials file (~/.aws/credentials)
        4. IAM role (when running on AWS infrastructure)
        
        Args:
            profile: Optional AWS profile name
            region: Optional AWS region
            
        Returns:
            Configured boto3 Session
            
        Raises:
            AuthenticationError: If no valid credentials found
        """
        try:
            # Create session with optional profile and region
            session = boto3.Session(
                profile_name=profile,
                region_name=region
            )
            
            # Verify credentials are available by attempting to get them
            # This will trigger the credential provider chain
            credentials = session.get_credentials()
            
            if credentials is None:
                raise AuthenticationError(
                    "No valid AWS credentials found. Please configure credentials using one of:\n"
                    "  1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)\n"
                    "  2. AWS credentials file (~/.aws/credentials)\n"
                    "  3. IAM role (when running on AWS infrastructure)\n"
                    "  4. AWS profile (specify in configuration)"
                )
            
            return session
            
        except ProfileNotFound as e:
            raise AuthenticationError(
                f"AWS profile '{profile}' not found. Please check your AWS configuration file (~/.aws/config)."
            ) from e
            
        except NoCredentialsError as e:
            raise AuthenticationError(
                "No valid AWS credentials found. Please configure credentials using one of:\n"
                "  1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)\n"
                "  2. AWS credentials file (~/.aws/credentials)\n"
                "  3. IAM role (when running on AWS infrastructure)\n"
                "  4. AWS profile (specify in configuration)"
            ) from e
            
        except PartialCredentialsError as e:
            raise AuthenticationError(
                f"Incomplete AWS credentials found: {str(e)}\n"
                "Please ensure both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set."
            ) from e
            
        except Exception as e:
            raise AuthenticationError(
                f"Failed to create AWS session: {str(e)}"
            ) from e
