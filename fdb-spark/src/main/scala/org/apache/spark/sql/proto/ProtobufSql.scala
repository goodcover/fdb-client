package org.apache.spark.sql.proto

import org.apache.spark.sql.catalyst.expressions.{ Expression, If, IsNull, Literal }
import org.apache.spark.sql.catalyst.expressions.objects.{ StaticInvoke, UnresolvedMapObjects }
import org.apache.spark.sql.types.{ ArrayType, DataType, MapType, ObjectType, StructType }
import scalapb.GeneratedMessageCompanion
import scalapb.descriptors.{ Descriptor, FieldDescriptor, PEmpty, PValue, ScalaType }
import scalapb.spark.{
  Implicits,
  JavaHelpers,
  MyCatalystToExternalMap,
  MyUnresolvedCatalystToExternalMap,
  ProtoSQL,
  SchemaOptions,
  TypedEncoders
}

abstract class ProtobufSql(override val schemaOptions: SchemaOptions) extends ProtoSQL(schemaOptions) { self =>

  override def schemaFor(descriptor: Descriptor): DataType = {
    val struct = StructType(descriptor.fields.map(structFieldFor))
    struct
  }

  override def dataTypeFor(fd: FieldDescriptor): DataType = if (fd.isMapField) fd.scalaType match {
    case ScalaType.Message(mapEntry) =>
      MapType(
        keyType = singularDataType(mapEntry.findFieldByNumber(1).get),
        valueType = singularDataType(mapEntry.findFieldByNumber(2).get),
        valueContainsNull = true // CHANGE-0: Made this more flexible
      )
    case _                           =>
      throw new RuntimeException(
        "Unexpected: field marked as map, but does not have an entry message associated"
      )
  }
  else if (fd.isRepeated) ArrayType(singularDataType(fd), containsNull = true) // CHANGE-0: Made this more flexible
  else singularDataType(fd)

  override val implicits: Implicits = new Implicits {
    val typedEncoders = new TypedEncoders {
      val protoSql = self

      override def fieldFromCatalyst(cmp: GeneratedMessageCompanion[?], fd: FieldDescriptor, input: Expression): Expression =
        if (fd.isRepeated && !fd.isMapField) {
          val objs = UnresolvedMapObjects(
            (input: Expression) => singleFieldValueFromCatalyst(cmp, fd, input),
            input
          )
          StaticInvoke(
            JavaHelpers.getClass,
            ObjectType(classOf[PValue]),
            "mkPRepeated",
            objs :: Nil
          )
        } else if (fd.isRepeated && fd.isMapField) {
          val mapEntryCmp = cmp.messageCompanionForFieldNumber(fd.number)
          val keyDesc     = mapEntryCmp.scalaDescriptor.findFieldByNumber(1).get
          val valDesc     = mapEntryCmp.scalaDescriptor.findFieldByNumber(2).get
          val urobjs      = MyUnresolvedCatalystToExternalMap(
            input,
            (in: Expression) => singleFieldValueFromCatalyst(mapEntryCmp, keyDesc, in),
            (in: Expression) => singleFieldValueFromCatalyst(mapEntryCmp, valDesc, in),
            protoSql.dataTypeFor(fd).asInstanceOf[MapType], // CHANGE-0: Point to this class not `ProtoSQL`
            classOf[Vector[(Any, Any)]]
          )
          val objs        = MyCatalystToExternalMap(urobjs)
          StaticInvoke(
            JavaHelpers.getClass,
            ObjectType(classOf[PValue]),
            "mkPRepeatedMap",
            Literal.fromObject(mapEntryCmp) ::
              objs :: Nil
          )
        } else if (fd.isOptional)
          If(
            IsNull(input),
            Literal.fromObject(PEmpty, ObjectType(classOf[PValue])),
            singleFieldValueFromCatalyst(cmp, fd, input)
          )
        else singleFieldValueFromCatalyst(cmp, fd, input)
    }
  }
}

object ProtobufSql extends ProtobufSql(SchemaOptions.Default)
