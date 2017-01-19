package com.berrycube.dev.hadoop.udf.geolcation;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;

import java.util.ArrayList;


/**
 * Created by wangxhere on 1/19/2017.
 */
// from http://rishavrohitblog.blogspot.sg/2014/09/hive-udf-to-get-latitude-and-longitude.html
@Description(name = "GeoEncodeUDF", value = "Get Lat-Lng", extended = "fetches location co-ordinates for given location from Google geocode Api and returns an ARRAY of 2 floats [lat,lng]")
@UDFType(deterministic = true)
public class GeoEncodeUDF extends GenericUDF {

    private ArrayList<FloatWritable> result;
    // Verify the input is of the required type.
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        // Exactly one input argument
        if( arguments.length != 1 ) {
            throw new UDFArgumentLengthException(GeoEncodeUDF.class.getSimpleName() + " accepts exactly one argument.");
        }
        // Is the input a String
        if (((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING ) {
            throw new UDFArgumentTypeException(0,"The single argument to " +GeoEncodeUDF.class.getSimpleName() + " should be String but " + arguments[0].getTypeName() + " is found");
        }
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // Should be exactly one argument
        if( arguments.length!=1 ) {
            return null;
        }
        // If passed a null, return a null
        if( arguments[0].get()==null ) {
            return null;
        }

        //		System.out.println("arguments[0].toString() is " + arguments[0].toString());
        //		System.out.println("arguments[0] is " + arguments[0].get());
        Float[] tmpLatLng = GeoLatLng.getLatLng(arguments[0].get().toString());
        //		System.out.println("LatLong are " + tmpLatLng[0] + "#" + tmpLatLng[1]);

        ArrayList<FloatWritable> result = new ArrayList<FloatWritable>();
        result.add(new FloatWritable(tmpLatLng[0]));
        result.add(new FloatWritable(tmpLatLng[1]));
        return result;
    }

    //	returns the string that will be returned when explain is used
    @Override
    public String getDisplayString(String[] arg0) {
        return new String("geo_points");
    }
}
