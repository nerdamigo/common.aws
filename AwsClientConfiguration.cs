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
