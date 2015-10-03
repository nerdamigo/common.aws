using NerdAmigo.Common.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NerdAmigo.Common.Aws
{
	public class AwsClientConfiguration : ICloneable
	{
		public string AccessKeyId { get; set;  }
		public string SecretAccessKey { get; set; }
		public string RegionEndpointSystemName { get; set; }

		public Amazon.RegionEndpoint GetEndpoint()
		{
			return Amazon.RegionEndpoint.GetBySystemName(this.RegionEndpointSystemName);
		}

		public override bool Equals(object obj)
		{
			var other = obj as AwsClientConfiguration;
			if (other == null)
			{
				return false;
			}

			return this.AccessKeyId == other.AccessKeyId &&
				this.SecretAccessKey == other.SecretAccessKey &&
				this.RegionEndpointSystemName == other.RegionEndpointSystemName;
		}

		public override int GetHashCode()
		{
			return this.AccessKeyId.GetHashCode() ^ 
				this.RegionEndpointSystemName.GetHashCode() ^ 
				this.SecretAccessKey.GetHashCode();
		}

		public object Clone()
		{
			return new AwsClientConfiguration() {
				AccessKeyId = this.AccessKeyId,
				SecretAccessKey = this.SecretAccessKey,
				RegionEndpointSystemName = this.RegionEndpointSystemName
			};
		}
	}
}
