package ru.mikhailsv;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.BitSet;

@Description(name = "PossibleIps")
public class PossibleIpsUDTF extends GenericUDTF {

    private StringObjectInspector net_ip_address_inspector;
    private StringObjectInspector subnet_mask_inspector;

    private Object[] forwardObjArray = new Object[2];

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if(args.length != 2){
            throw new UDFArgumentException(getClass().getSimpleName() + " takes 2 arguments!");
        }
        net_ip_address_inspector = (StringObjectInspector) args[0];
        subnet_mask_inspector = (StringObjectInspector) args[1];

        List<String> fieldNames = new ArrayList<String>();
        fieldNames.add("ip");
        fieldNames.add("value");

        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        fieldInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

        String net_ip_address = net_ip_address_inspector.getPrimitiveJavaObject(objects[0]);
        String subnet_mask = subnet_mask_inspector.getPrimitiveJavaObject(objects[1]);

        if (net_ip_address == null || subnet_mask == null) {
            forwardObjArray[0] = null;
            forwardObjArray[1] = 0;
            forward(forwardObjArray);
        } else {
            try {
                BitSet mask_bs = getBitsetByBinary(getBinaryString(subnet_mask));
                BitSet net_ip_bs = getBitsetByBinary(getBinaryString(net_ip_address));
                generateAllPossibleIps(mask_bs, net_ip_bs);
            } catch (UnknownHostException e) {
                forwardObjArray[0] = null;
                forward(forwardObjArray);
            }
        }
    }

    @Override
    public void close() throws HiveException {
    }

    private static String getBinaryString(String ipv4) throws UnknownHostException {
        InetAddress i = InetAddress.getByName(ipv4);
        int intRepresentation = ByteBuffer.wrap(i.getAddress()).getInt();
        return Integer.toBinaryString(intRepresentation);
    }

    private static String getIp(BitSet binary) throws UnknownHostException {
        InetAddress i = InetAddress.getByName(String.valueOf(binary.toLongArray()[0]));
        return i.getHostAddress();
    }

    private static BitSet getBitsetByBinary(String binary) {
        BitSet bs = new BitSet(32);
        for (int i = 0; i < binary.length(); ++i) {
            bs.set(i, binary.charAt(binary.length() - i - 1) == '1');
        }
        return bs;
    }

    private void generateAllPossibleIps(BitSet mask_bs, BitSet net_ip_bs) throws UnknownHostException, HiveException {
        for (int i = 1; i < Math.pow(2, mask_bs.nextSetBit(0)) - 1; ++i) {
            BitSet bs = BitSet.valueOf(new long[]{i});
            bs.or(net_ip_bs);
            forwardObjArray[0] = getIp(bs);
            forwardObjArray[1] = bs.toLongArray()[0];
            forward(forwardObjArray);
        }
    }
}