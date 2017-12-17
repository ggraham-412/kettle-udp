package com.hitachivantara.udpsender;

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.hitachivantara.message.PacketDecoder;
import com.hitachivantara.network.UDPSender;
import com.hitachivantara.objectpool.ObjectPool.PoolItem;

public class UDPSenderStep extends BaseStep implements StepInterface {

	 public UDPSenderStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
	     Trans trans ) {
	   super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
	 }
	 
	 public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

	     UDPSenderData uData = (UDPSenderData)sdi;
	     UDPSenderMeta uMeta = (UDPSenderMeta)smi;

	     if ( !isStopped() ) {

		     Object[] row = getRow();
		     if ( row == null ) {  
		    	 setOutputDone();
		    	 return false;
		     }

		     if ( first ) {
		       first = false;		       
		       uData.m_inputRowMeta = getInputRowMeta();
		       uData.m_fieldMap = new HashMap<Integer,Integer>();
		       String[] fieldNames = uMeta.getFieldNames();
		       for ( int ii = 0; ii < fieldNames.length; ii++ ) {
		    	   uData.m_fieldMap.put(ii, uData.m_inputRowMeta.indexOfValue(fieldNames[ii]));
		       }
		       configureConnection(uMeta, uData);
		     }


		       String[] fieldNames = uMeta.getFieldNames();
		     Object[] toSend = new Object[fieldNames.length];
		       for ( int ii = 0; ii < fieldNames.length; ii++ ) {
		    	   toSend[ii] = row[uData.m_fieldMap.get(ii)];
		       }
	
		       PoolItem<ByteBuffer> pBuffer = uData.m_sender.getByteBuffer();
		       uData.m_decoder.EncodePacket(toSend, pBuffer.getPoolItem());
		       uData.m_sender.send(pBuffer);
		       logBasic("Sent Message");
		     
		     return true;
		   } else {
		     setStopped( true );
		     return false;
		   }
		 }
	 
	 protected void configureConnection( UDPSenderMeta meta, UDPSenderData data ) throws KettleException {
	     if ( data.m_sender == null ) {
		   
	  	   logBasic("Running configureConnection()...");
		     String address = environmentSubstitute( meta.getAddress() );
		     if ( Const.isEmpty( address ) ) {
		       throw new KettleException(
		           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.NoIPAddress" ) );
		     }

		     String sPort = environmentSubstitute( meta.getPort() );
		     if ( Const.isEmpty( sPort ) ) {
		       throw new KettleException(
		           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.NoPort" ) );
		     }
		     int port = 0;
		     try {
		    	 port = Integer.parseInt(sPort);
		     }
		     catch ( NumberFormatException ex) {
			       throw new KettleException(
				           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.BadPort", sPort ) );	    	 
		     }
		  	   logBasic("IP Address is " + address + ":" + sPort);

		  	   data.m_decoder = new PacketDecoder();
		  	   for ( PacketDecoder.PacketFieldConfig f : meta.getFields() ) {
		  		   data.m_decoder.addField(f);
		  	   }
		  	   	     
		  	   String sInitPoolSize = environmentSubstitute ( meta.getInitPoolSize() );
		  	   if ( Const.isEmpty(sInitPoolSize ) ) {
			       throw new KettleException(
				           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.NoInitPoolSize" ) );	  		   
		  	   }
		  	   int initPoolSize = 0;
		  	   try {
		  		 initPoolSize = Integer.parseInt(sInitPoolSize);
		  	   }
			     catch ( NumberFormatException ex) {
				       throw new KettleException(
					           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.BadInitPoolSizeSize", sInitPoolSize ) );	    	 
			     }
		  	   logBasic("Initial Object Pool size is " + sInitPoolSize + " objects");
	     
		  	   String sMaxPoolSize = environmentSubstitute ( meta.getMaxPoolSize() );
		  	   if ( Const.isEmpty(sMaxPoolSize ) ) {
			       throw new KettleException(
				           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.NoMaxPoolSize" ) );	  		   
		  	   }
		  	   int maxPoolSize = 0;
		  	   try {
		  		 maxPoolSize = Integer.parseInt(sMaxPoolSize);
		  	   }
			     catch ( NumberFormatException ex) {
				       throw new KettleException(
					           BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderStep.Error.BadInitPoolSizeSize", sMaxPoolSize ) );	    	 
			     }
		  	   logBasic("Max Object Pool size is " + sMaxPoolSize + " objects");
			     data.m_sender = new UDPSender(address, port,initPoolSize, maxPoolSize);
			  	   logBasic("Created receiver " + (data.m_sender != null));
	     
	     }
	 }


}
