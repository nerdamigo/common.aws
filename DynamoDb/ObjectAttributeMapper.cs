using Amazon.DynamoDBv2.Model;
using NerdAmigo.Abstractions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace NerdAmigo.Common.Aws.DynamoDb
{
	public class ObjectAttributeMapper<TObject> where TObject : new()
	{
		private static object toDictionaryLocker;
		private static object toObjectLocker;

		private static Expression<Func<TObject, Dictionary<string, AttributeValue>>> CachedMapperToDictionaryExpression;
		private static Func<TObject, Dictionary<string, AttributeValue>> CachedMapperToDictionary;

		private static Expression<Func<Dictionary<string, AttributeValue>, TObject>> CachedMapperToObjectExpression;
		private static Func<Dictionary<string, AttributeValue>, TObject> CachedMapperToObject;

		static ObjectAttributeMapper()
		{
			toDictionaryLocker = new object();
			toObjectLocker = new object();
		}

		public Dictionary<string, AttributeValue> MapToDictionary(TObject Source)
		{
			var mapper = GetMapperToDictionary();
			return mapper(Source);
		}

		public TObject MapToObject(Dictionary<string, AttributeValue> Attributes)
		{
			Func<Dictionary<string, AttributeValue>, TObject> mapper = GetMapperToObject();
			return mapper(Attributes);
		}

		private Func<Dictionary<string, AttributeValue>, TObject> GetMapperToObject()
		{
			lock (toObjectLocker)
			{
				if (CachedMapperToObject != null)
				{
					return CachedMapperToObject;
				}
				else
				{
					CachedMapperToObject = CreateMapperToObject();
					return CachedMapperToObject;
				}
			}
			throw new NotImplementedException();
		}

		private Func<Dictionary<string, AttributeValue>, TObject> CreateMapperToObject()
		{
			Type objType = typeof(TObject);
			PropertyInfo[] objProps = objType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

			ParameterExpression mapperParameter = Expression.Parameter(typeof(Dictionary<string, AttributeValue>), "source");
			ParameterExpression result = Expression.Parameter(objType, "result");

			List<Expression> blockExpressions = new List<Expression>();

			//create the object
			blockExpressions.Add(Expression.Assign(result, Expression.New(objType)));

			foreach (PropertyInfo objProp in objProps)
			{
				Expression objectValueExpression = GetAttributeValueToObjectValueExpression(objProp, mapperParameter);
				var setMethod = objProp.GetSetMethod(true);
				if (setMethod != null)
				{
					blockExpressions.Add(Expression.Call(result, setMethod, objectValueExpression));
				}
			}

			blockExpressions.Add(result);

			BlockExpression block = Expression.Block(
				new[] { result },
				blockExpressions
			);

			CachedMapperToObjectExpression = Expression.Lambda<Func<Dictionary<string, AttributeValue>, TObject>>(block, mapperParameter);
			return CachedMapperToObjectExpression.Compile();
		}

		private Expression GetAttributeValueToObjectValueExpression(PropertyInfo objProp, ParameterExpression mapperParameter)
		{
			Type propType = objProp.PropertyType;
			
			//have to get the item from the dictionary!
			MethodInfo attributeGetMethod = typeof(Dictionary<string, AttributeValue>).GetMethod("TryGetValue");

			ParameterExpression attributeValueExpression = Expression.Variable(typeof(AttributeValue), "attributeValue");
			Expression attributeGetExpression = Expression.Call(mapperParameter, attributeGetMethod, Expression.Constant(objProp.Name), attributeValueExpression);

			ParameterExpression propertyValueExpression = Expression.Parameter(propType, "propertyValue");
			Expression parseExpression = null;

			Type underlyingType = propType;
			bool isNullable = propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>);
			if (isNullable)
			{
				underlyingType = propType.GenericTypeArguments[0];
			}

			if (underlyingType.IsValueType)
			{
				//https://msdn.microsoft.com/en-us/library/bfft1t3c.aspx
				if (typeof(bool) == underlyingType)
				{
					//boolean
					parseExpression = Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("BOOL"));
				}

				if (
					typeof(decimal) == underlyingType ||
					typeof(byte) == underlyingType ||
					typeof(double) == underlyingType ||
					typeof(float) == underlyingType ||
					typeof(int) == underlyingType ||
					typeof(long) == underlyingType ||
					typeof(sbyte) == underlyingType ||
					typeof(short) == underlyingType ||
					typeof(uint) == underlyingType ||
					typeof(ulong) == underlyingType ||
					typeof(ushort) == underlyingType
				)
				{
					//numeric
					parseExpression = Expression.Convert(
						Expression.Call(
							underlyingType.GetMethod("Parse", new Type[] { typeof(string) }),
							Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("N"))
						),
						propType
					);
				}
			}

			if(underlyingType == typeof(string))
			{
				parseExpression = Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("S"));
			}

			if(underlyingType == typeof(DateTime))
			{
				parseExpression = Expression.Call(
					underlyingType.GetMethod("Parse", new Type[] { typeof(string) }),
					Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("S"))
				);
			}
			
			//if list
			if (parseExpression == null && typeof(IEnumerable).IsAssignableFrom(underlyingType))
			{
				//treat as a list
				parseExpression = Expression.Default(propType);
			}
			else if (parseExpression == null && typeof(Identifier).IsAssignableFrom(underlyingType))
			{
				ConstructorInfo identifierConstructor = underlyingType.GetConstructor(new[] { typeof(string) });
				if(identifierConstructor != null){
					parseExpression = Expression.New(identifierConstructor, Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("S")));
				}
			}
			else if (parseExpression == null && propType.IsClass)
			{
				//construct an ObjectAttributeMapper
				Type objMapperType = typeof(ObjectAttributeMapper<>).MakeGenericType(propType);
				ParameterExpression objMapper = Expression.Parameter(objMapperType, "objMapper");

				//set the attributeValueExpression to the result of the MapToDictionary of the constructed obj
				BlockExpression objBlock = Expression.Block(
					new[] { objMapper },
					Expression.Assign(objMapper, Expression.New(objMapperType)),
					Expression.Call(
						objMapper, 
						objMapperType.GetMethod("MapToObject", new[] { typeof(Dictionary<string, AttributeValue>) }),
						Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("M")))
				);

				parseExpression = objBlock;
			} 
			else if( parseExpression == null)
			{
				parseExpression = Expression.Default(propType);
			}

			Expression ifHasAttributeExpression = Expression.IfThenElse(
				attributeGetExpression,
				Expression.IfThenElse(
					Expression.Property(attributeValueExpression, typeof(AttributeValue).GetProperty("NULL")),
					Expression.Assign(propertyValueExpression, Expression.Default(propType)),
					Expression.Assign(propertyValueExpression, parseExpression)
				),
				Expression.Assign(propertyValueExpression, Expression.Default(propType))
			);

			return Expression.Block(
				new[] { propertyValueExpression, attributeValueExpression },
				ifHasAttributeExpression,
				propertyValueExpression
			);
		}

		private Func<TObject, Dictionary<string, AttributeValue>> GetMapperToDictionary()
		{
			lock (toDictionaryLocker)
			{
				if (CachedMapperToDictionary != null)
				{
					return CachedMapperToDictionary;
				}
				else
				{
					CachedMapperToDictionary = CreateMapperToDictionary();
					return CachedMapperToDictionary;
				}
			}
		}

		/// <summary>
		/// Build mapping function for converting a given object to a dictionary of attribute values
		/// </summary>
		/// <param name="Source"></param>
		/// <returns></returns>
		private Func<TObject, Dictionary<string, AttributeValue>> CreateMapperToDictionary()
		{
			Type objType = typeof(TObject);
			PropertyInfo[] objProps = objType.GetProperties(BindingFlags.Public | BindingFlags.Instance);

			ParameterExpression mapperParameter = Expression.Parameter(typeof(TObject), "source");

			ParameterExpression result = Expression.Parameter(typeof(Dictionary<string, AttributeValue>), "result");

			List<Expression> blockExpressions = new List<Expression>();
			
			//create the dictionary
			blockExpressions.Add(Expression.Assign(result, Expression.New(typeof(Dictionary<string, AttributeValue>))));

			//add item to dictionary for each property
			MethodInfo addItemMethod = typeof(Dictionary<string, AttributeValue>).GetMethod("Add");

			foreach(PropertyInfo objProp in objProps)
			{
				ConstantExpression propNameExpression = Expression.Constant(objProp.Name);
				Expression attributeExpression = GetPropertyToAttributeValueExpression(objProp, mapperParameter);
				blockExpressions.Add(Expression.Call(result, addItemMethod, propNameExpression, attributeExpression));
			}

			blockExpressions.Add(result);

			BlockExpression block = Expression.Block(
				new[] { result },
				blockExpressions
			);
			
			CachedMapperToDictionaryExpression = Expression.Lambda<Func<TObject, Dictionary<string, AttributeValue>>>(block, mapperParameter);
			return CachedMapperToDictionaryExpression.Compile();
		}

		private Expression GetPropertyToAttributeValueExpression(PropertyInfo objProp, ParameterExpression mapperParameter)
		{
			//construct a block for determining the attribute value expression to run
			ParameterExpression attributeParameter = Expression.Parameter(typeof(AttributeValue), "attribute");
			NewExpression attributeValueCtorExpression = Expression.New(typeof(AttributeValue));
			Expression objValueExpression = Expression.Property(mapperParameter, objProp);
			
			//abstraction that handles any type conversions
			Expression attributeValueExpression = objValueExpression;
			Expression isNullExpression = Expression.Constant(false);

			//set a default expression
			string setMethodName = null;

			Type propType = objProp.PropertyType;
			Type underlyingType = propType;
			bool isNullable = propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>);
			if (isNullable)
			{
				underlyingType = propType.GenericTypeArguments[0];
			}

			if(underlyingType.IsValueType)
			{ 
				//https://msdn.microsoft.com/en-us/library/bfft1t3c.aspx
				if (typeof(bool) == underlyingType)
				{
					//boolean
					setMethodName = "BOOL";
				}

				if(
					typeof(decimal) == underlyingType ||
					typeof(byte) == underlyingType ||
					typeof(double) == underlyingType ||
					typeof(float) == underlyingType ||
					typeof(int) == underlyingType ||
					typeof(long) == underlyingType ||
					typeof(sbyte) == underlyingType ||
					typeof(short) == underlyingType ||
					typeof(uint) == underlyingType ||
					typeof(ulong) == underlyingType ||
					typeof(ushort) == underlyingType
				)
				{
					//numeric
					setMethodName = "N";
					attributeValueExpression = Expression.Call(objValueExpression, typeof(object).GetMethod("ToString"));

					if (isNullable)
					{
						isNullExpression = Expression.Equal(Expression.Constant(null), objValueExpression);
					}
				}
			}

			if(underlyingType == typeof(string))
			{
				setMethodName = "S";
				isNullExpression = Expression.Equal(Expression.Constant(null), objValueExpression);
			}

			if(underlyingType == typeof(DateTime))
			{
				//date
				setMethodName = "S";
				attributeValueExpression = Expression.Call(objValueExpression, propType.GetMethod("ToString", new[] { typeof(string) }), Expression.Constant("o"));

				if (isNullable)
				{
					isNullExpression = Expression.Equal(Expression.Constant(null), objValueExpression);
				}
			}

			if(typeof(IEnumerable).IsAssignableFrom(underlyingType))
			{
				//treat as a list
			}
			else if(typeof(Identifier).IsAssignableFrom(underlyingType))
			{
				setMethodName = "S";
				attributeValueExpression = Expression.Call(objValueExpression, propType.GetMethod("ToString"));
			}
			else if(String.IsNullOrEmpty(setMethodName) && propType.IsClass)
			{
				//create a map attribute, assign it's value to that of the MapToDictionary for the value
				setMethodName = "M";
				isNullExpression = Expression.Equal(Expression.Constant(null), objValueExpression);

				//construct an ObjectAttributeMapper
				Type objMapperType = typeof(ObjectAttributeMapper<>).MakeGenericType(propType);
				ParameterExpression objMapper = Expression.Parameter(objMapperType, "objMapper");

				//set the attributeValueExpression to the result of the MapToDictionary of the constructed obj
				BlockExpression objBlock = Expression.Block(
					new[] { objMapper },
					Expression.Assign(objMapper, Expression.New(objMapperType)),
					Expression.Call(objMapper, objMapperType.GetMethod("MapToDictionary", new[] { propType }), objValueExpression)
				);

				attributeValueExpression = objBlock;
			}

			if (!String.IsNullOrEmpty(setMethodName))
			{
				//wrap in null check
				attributeValueExpression = 
					Expression.IfThenElse(
						isNullExpression,
						Expression.Call(attributeParameter, typeof(AttributeValue).GetProperty("NULL").GetSetMethod(), Expression.Constant(true)),
						Expression.Call(attributeParameter, typeof(AttributeValue).GetProperty(setMethodName).GetSetMethod(), attributeValueExpression)
				);
			}
			else
			{
				attributeValueExpression = Expression.Call(attributeParameter, typeof(AttributeValue).GetProperty("S").GetSetMethod(), Expression.Constant("Unrecognized Type"));
			}

			BlockExpression attributeBlock = Expression.Block(
				new[] { attributeParameter },
				Expression.Assign(attributeParameter, attributeValueCtorExpression),
				attributeValueExpression,
				attributeParameter
			);

			return attributeBlock;
		}
	}
}
