package org.ggraham.udpreceiver;

import org.ggraham.message.IHandleMessage;
import org.ggraham.message.PacketDecoder;
import org.ggraham.network.UDPReceiver;
import org.ggraham.networksenderreceiver.PackageService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.spoon.SharedObjectSyncUtil.SynchronizationHandler;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class UDPReceiverStep extends BaseStep implements StepInterface {

 public UDPReceiverStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
     Trans trans ) {
   super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
 }

 public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

   if ( !isStopped() ) {

     if ( first ) {
       first = false;

       if ( ( (UDPReceiverData) sdi ).m_executionDuration > 0 ) {
           ( (UDPReceiverData) sdi ).m_startTime = new Date();
         }

       if ( ( (UDPReceiverData) sdi ).m_packetCount > 0 ) {
           ( (UDPReceiverData) sdi ).m_currentPacketCount = 0;
         }

       ( (UDPReceiverData) sdi ).m_outputRowMeta = new RowMeta();

       smi.getFields( ( (UDPReceiverData) sdi ).m_outputRowMeta, getStepname(), null, null, getTransMeta(), null,
           null );
     }

     if ( ( (UDPReceiverData) sdi ).m_executionDuration > 0 ) {
         if ( System.currentTimeMillis() - ( (UDPReceiverData) sdi ).m_startTime.getTime()
             > ( (UDPReceiverData) sdi ).m_executionDuration * 1000 ) {
           setOutputDone();
           return false;
         }
       }

     if ( ( (UDPReceiverData) sdi ).m_packetCount > 0 ) {
         if ( ( (UDPReceiverData) sdi ).m_currentPacketCount >= ( (UDPReceiverData) sdi ).m_packetCount ) {
           setOutputDone();
           return false;
         }
       }

     return true;
   } else {
     setStopped( true );
     return false;
   }
 }

 protected synchronized void shutdown( UDPReceiverData data ) {
   if ( data.m_receiver != null ) {
     try {
       logBasic( "Stopping UDP receiver..." );
       data.m_receiver.stop();
       data.m_receiver = null;
     } catch ( Exception e ) {
       logError( BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.Shutdown" ), e );
     }
   }
 }

 public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
   if ( super.init( smi, sdi ) ) {

	   logBasic("Running init()...");
	 PackageService.GetImpl().setLoggerImpl(new PackageService.DefaultLogger() {		
		@Override
		protected void doLog(String message) {
			logBasic(message);
		}
	});
	   
     try {
       configureConnection( (UDPReceiverMeta) smi, (UDPReceiverData) sdi );
       String runFor = ( (UDPReceiverMeta) smi ).getMaxDuration();
       try {
         ( (UDPReceiverData) sdi ).m_executionDuration = Long.parseLong( runFor );
       } catch ( NumberFormatException e ) {
         logError( e.getMessage(), e );
         return false;
       }
       String runCount = ( (UDPReceiverMeta) smi ).getMaxPackets();
       try {
         ( (UDPReceiverData) sdi ).m_packetCount = Long.parseLong( runCount );
       } catch ( NumberFormatException e ) {
         logError( e.getMessage(), e );
         return false;
       }
     } catch ( KettleException e ) {
       logError( e.getMessage(), e );
       return false;
     }

     ((UDPReceiverData)sdi).m_receiver.start();
     return true;
   }

   return false;
 }

 public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
   UDPReceiverData data = (UDPReceiverData) sdi;

   shutdown( data );
   super.dispose( smi, sdi );
 }

 public void stopRunning( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

   UDPReceiverData data = (UDPReceiverData) sdi;
   shutdown( data );
   super.stopRunning( smi, sdi );
 }

 protected void configureConnection( UDPReceiverMeta meta, UDPReceiverData data ) throws KettleException {
     if ( data.m_receiver == null ) {
	   
  	   logBasic("Running configureConnection()...");
	     String address = environmentSubstitute( meta.getAddress() );
	     if ( Const.isEmpty( address ) ) {
	       throw new KettleException(
	           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoIPAddress" ) );
	     }

	     String sPort = environmentSubstitute( meta.getPort() );
	     if ( Const.isEmpty( sPort ) ) {
	       throw new KettleException(
	           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoPort" ) );
	     }
	     int port = 0;
	     try {
	    	 port = Integer.parseInt(sPort);
	     }
	     catch ( NumberFormatException ex) {
		       throw new KettleException(
			           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.BadPort", sPort ) );	    	 
	     }
	  	   logBasic("IP Address is " + address + ":" + sPort);

	  	   data.m_decoder = new PacketDecoder();
	  	   for ( PacketDecoder.PacketFieldConfig f : meta.getFields() ) {
	  		   data.m_decoder.addField(f);
	  	   }
	     data.m_receiver = new UDPReceiver(address, port, meta.getBigEndian(), new HandlerCallback(meta, data));
	  	   logBasic("Created receiver " + (data.m_receiver != null));
	  	   
	  	   String sBufferSize = environmentSubstitute ( meta.getBufferSize() );
	  	   if ( Const.isEmpty(sBufferSize ) ) {
		       throw new KettleException(
			           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoBufferSize" ) );	  		   
	  	   }
	  	   int bufsize = 0;
	  	   try {
	  		   bufsize = Integer.parseInt(sBufferSize);
	  	   }
		     catch ( NumberFormatException ex) {
			       throw new KettleException(
				           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.BadBufferSize", sBufferSize ) );	    	 
		     }
	  	   logBasic("Recv buffer size is " + sBufferSize + " packets");
	  	   data.m_receiver.setBufferSize(bufsize);
     
	  	   String sInitPoolSize = environmentSubstitute ( meta.getInitPoolSize() );
	  	   if ( Const.isEmpty(sInitPoolSize ) ) {
		       throw new KettleException(
			           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoInitPoolSize" ) );	  		   
	  	   }
	  	   int initPoolSize = 0;
	  	   try {
	  		 initPoolSize = Integer.parseInt(sInitPoolSize);
	  	   }
		     catch ( NumberFormatException ex) {
			       throw new KettleException(
				           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.BadInitPoolSizeSize", sInitPoolSize ) );	    	 
		     }
	  	   logBasic("Initial Object Pool size is " + sInitPoolSize + " objects");
	  	   data.m_receiver.setPoolInitSize(initPoolSize);
     
	  	   String sMaxPoolSize = environmentSubstitute ( meta.getMaxPoolSize() );
	  	   if ( Const.isEmpty(sMaxPoolSize ) ) {
		       throw new KettleException(
			           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoMaxPoolSize" ) );	  		   
	  	   }
	  	   int maxPoolSize = 0;
	  	   try {
	  		 maxPoolSize = Integer.parseInt(sMaxPoolSize);
	  	   }
		     catch ( NumberFormatException ex) {
			       throw new KettleException(
				           BaseMessages.getString( UDPReceiverMeta.PKG, "UDPReceiverStep.Error.BadInitPoolSizeSize", sMaxPoolSize ) );	    	 
		     }
	  	   logBasic("Max Object Pool size is " + sMaxPoolSize + " objects");
	  	   data.m_receiver.setPoolMaxSize(maxPoolSize);
     
     }
 }

 protected class HandlerCallback implements IHandleMessage<ByteBuffer> {

	 private UDPReceiverMeta m_meta;
	 private UDPReceiverData m_data;	 
	 
	 public HandlerCallback(UDPReceiverMeta meta, UDPReceiverData data) {
		 m_meta = meta;
		 m_data = data;
	 }
	 
	 public boolean handleMessage(ByteBuffer message) {
	        Object[] outRow = RowDataUtil.allocateRowData( m_data.m_outputRowMeta.size() );
		 try {
		 
        m_data.m_decoder.DecodePacket(message, outRow);
		 }
		 catch(Exception ex) {
			 return false;
		 }
         try {
			putRow( m_data.m_outputRowMeta, outRow ); // putRow is synched according to javadoc
			synchronized (m_data.m_lock) {
				m_data.m_currentPacketCount++;				
			}
		 } catch (KettleStepException e) {
			return false;
		 }
         return true;
     }

   }
}
