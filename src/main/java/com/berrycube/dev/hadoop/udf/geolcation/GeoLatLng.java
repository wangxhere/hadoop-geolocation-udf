package com.berrycube.dev.hadoop.udf.geolcation;

import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

// developed from http://rishavrohitblog.blogspot.sg/2014/09/hive-udf-to-get-latitude-and-longitude.html
public class GeoLatLng {
    public static Float[] getLatLng(String location) throws HiveException {
        //		String geoPoints = null;
        Float[] geoPoints = new Float[] {null, null};
        // if input is null return null array
        if (location == null ) return null;

        String loc_uri = null;


        try {
            loc_uri = URIUtil.encodeQuery("http://maps.googleapis.com/maps/api/geocode/json?address=" + location);
        } catch (URIException e) {
            throw new HiveException(e);
        }

        // Create an instance of HttpClient.
        HttpClient client = new HttpClient();

        // Create a method instance.
        GetMethod method = new GetMethod(loc_uri);

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));


        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }

            // Read the response body.
            byte[] responseBody = method.getResponseBody();
            String responseStr = new String(responseBody);

            JSONObject response = new JSONObject(responseStr);
            JSONObject latlng = response.getJSONArray("results")
                    .getJSONObject(0).getJSONObject("geometry")
                    .getJSONObject("location");

            try {
                geoPoints[0] = new Float(latlng.get("lat").toString());
                geoPoints[1] = new Float(latlng.get("lng").toString());
            } catch (Exception e) {
                System.err.println("Got error in decoding result: " + latlng.toString());
                geoPoints[0] = null;
                geoPoints[1] = null;
            }

            return geoPoints;
        } catch (HttpException e) {
            System.err.println("Fatal protocol violation: " + e.getMessage());
            throw new HiveException(e);
        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            throw new HiveException(e);
        } catch (JSONException e) {
            return geoPoints;
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
//        return geoPoints;
    }
}