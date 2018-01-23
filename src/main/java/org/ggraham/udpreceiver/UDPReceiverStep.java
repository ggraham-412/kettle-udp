package org.ggraham.udpreceiver;

	/*
	 * 
	 * Apache License 2.0 
	 * 
	 * Copyright (c) [2017] [Gregory Graham]
	 * 
	 * See LICENSE.txt for details.
	 * 
	 */
import org.ggraham.ggutils.logging.DefaultLogger;
import org.ggraham.ggutils.logging.LogLevel;
import org.ggraham.ggutils.message.IHandleMessage;
import org.ggraham.ggutils.message.PacketDecoder;
import org.ggraham.ggutils.message.PacketDecoder.FieldType;
import org.ggraham.ggutils.message.PacketDecoder.PacketFieldConfig;
import org.ggraham.ggutils.network.UDPReceiver;
import org.ggraham.ggutils.PackageService;
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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 
 * Implements StepInterface for UDPReceiver: receives UDP packets, decodes them
 * and puts rows containing data from the UDP packets into the transformation.
 * 
 * @author ggraham
 *
 */
public class UDPReceiverStep extends BaseStep implements StepInterface {

	public UDPReceiverStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	// Since this step is a "source" of data, there are no rows to process.
	// This method is used to tell the transformation when to stop.
	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		if (isStopped()) {
			// If transformation stopped externally, we're done.
			return false;
		} else {

			// Check if this is the first time through
			if (first) {

				first = false;

				// Initialize the data
				((UDPReceiverData) sdi).m_outputRowMeta = new RowMeta();
				smi.getFields(((UDPReceiverData) sdi).m_outputRowMeta, getStepname(), null, null, getTransMeta(), null,
						null);

				if (((UDPReceiverData) sdi).m_executionDuration > 0) {
					((UDPReceiverData) sdi).m_startTime = new Date();
				}

				if (((UDPReceiverData) sdi).m_packetCount > 0) {
					((UDPReceiverData) sdi).m_currentPacketCount = 0;
				}

			}

			// If livetime is exceeded, stop.
			if (((UDPReceiverData) sdi).m_executionDuration > 0) {
				if (System.currentTimeMillis()
						- ((UDPReceiverData) sdi).m_startTime.getTime() > ((UDPReceiverData) sdi).m_executionDuration
								* 1000) {
					setOutputDone();
					return false;
				}
			}

			// If packet count is exceeded, stop
			if (((UDPReceiverData) sdi).m_packetCount > 0) {
				if (((UDPReceiverData) sdi).m_currentPacketCount >= ((UDPReceiverData) sdi).m_packetCount) {
					setOutputDone();
					return false;
				}
			}

			// We can keep going
			return true;
		}
	}

	
	@Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		// Shutdown the UDPReceiver and call super
		UDPReceiverData data = (UDPReceiverData) sdi;
		shutdown(data);
		super.dispose(smi, sdi);
	}

	@Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		if (super.init(smi, sdi)) {

			logBasic("Running UDPReceiver init()...");
			final BaseStep bStep = this;
			PackageService.getPackageService().setLogImpl(new DefaultLogger() {
				@Override
				protected void doLog(String source, String message, int setLevel, int logLevel, String sLogLevel) {
				    bStep.logBasic(message);
				}
			});
			PackageService.getLog().setLogLevel(LogLevel.BASIC);

			try {
				configureConnection((UDPReceiverMeta) smi, (UDPReceiverData) sdi);
				String runFor = ((UDPReceiverMeta) smi).getMaxDuration();
				try {
					((UDPReceiverData) sdi).m_executionDuration = Long.parseLong(runFor);
				} catch (NumberFormatException e) {
					logError(e.getMessage(), e);
					return false;
				}
				String runCount = ((UDPReceiverMeta) smi).getMaxPackets();
				try {
					((UDPReceiverData) sdi).m_packetCount = Long.parseLong(runCount);
				} catch (NumberFormatException e) {
					logError(e.getMessage(), e);
					return false;
				}
			} catch (KettleException e) {
				logError(e.getMessage(), e);
				return false;
			}

			((UDPReceiverData) sdi).m_receiver.start();
			return true;
		}

		return false;
	}

	@Override
	public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		UDPReceiverData data = (UDPReceiverData) sdi;
		shutdown(data);
		super.stopRunning(smi, sdi);
	}

	protected synchronized void shutdown(UDPReceiverData data) {
		if (data.m_receiver != null) {
			try {
				logBasic("Stopping UDP receiver...");
				data.m_receiver.stop();
				data.m_receiver = null;
			} catch (Exception e) {
				logError(BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.Shutdown"), e);
			}
		}
	}

	protected void configureConnection(UDPReceiverMeta meta, UDPReceiverData data) throws KettleException {
		if (data.m_receiver == null) {

			logBasic("Running configureConnection()...");
			String address = environmentSubstitute(meta.getAddress());
			if (Const.isEmpty(address)) {
				throw new KettleException(
						BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoIPAddress"));
			}

			String sPort = environmentSubstitute(meta.getPort());
			if (Const.isEmpty(sPort)) {
				throw new KettleException(BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoPort"));
			}
			int port = 0;
			try {
				port = Integer.parseInt(sPort);
			} catch (NumberFormatException ex) {
				throw new KettleException(
						BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.BadPort", sPort));
			}
			logBasic("IP Address is " + address + ":" + sPort);

			data.m_decoder = new PacketDecoder();
			for (PacketDecoder.PacketFieldConfig f : meta.getFields()) {
				data.m_decoder.addField(f);
				logBasic("  Adding field: " + f.toString());
			}
			logBasic("Big endian: " + meta.getBigEndian());
			data.m_receiver = new UDPReceiver(address, port, meta.getBigEndian(), new HandlerCallback(meta, data));
			logBasic("Created receiver " + (data.m_receiver != null));

			String sBufferSize = environmentSubstitute(meta.getBufferSize());
			if (Const.isEmpty(sBufferSize)) {
				throw new KettleException(
						BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoBufferSize"));
			}
			int bufsize = 0;
			try {
				bufsize = Integer.parseInt(sBufferSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(UDPReceiverMeta.PKG,
						"UDPReceiverStep.Error.BadBufferSize", sBufferSize));
			}
			logBasic("Recv buffer size is " + sBufferSize + " packets");
			data.m_receiver.setBufferSize(bufsize);

			String sInitPoolSize = environmentSubstitute(meta.getInitPoolSize());
			if (Const.isEmpty(sInitPoolSize)) {
				throw new KettleException(
						BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoInitPoolSize"));
			}
			int initPoolSize = 0;
			try {
				initPoolSize = Integer.parseInt(sInitPoolSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(UDPReceiverMeta.PKG,
						"UDPReceiverStep.Error.BadInitPoolSizeSize", sInitPoolSize));
			}
			logBasic("Initial Object Pool size is " + sInitPoolSize + " objects");
			data.m_receiver.setPoolInitSize(initPoolSize);

			String sMaxPoolSize = environmentSubstitute(meta.getMaxPoolSize());
			if (Const.isEmpty(sMaxPoolSize)) {
				throw new KettleException(
						BaseMessages.getString(UDPReceiverMeta.PKG, "UDPReceiverStep.Error.NoMaxPoolSize"));
			}
			int maxPoolSize = 0;
			try {
				maxPoolSize = Integer.parseInt(sMaxPoolSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(UDPReceiverMeta.PKG,
						"UDPReceiverStep.Error.BadInitPoolSizeSize", sMaxPoolSize));
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

		// PDI has not FLOAT or INT, so we convert them here 
		private void reconfarbulate(Object[] outrow) {
			PacketFieldConfig[] fields = m_meta.getFields();
			for ( int i = 0; i < fields.length; i++ ) {
				if ( fields[i].getFieldType() == FieldType.FLOAT ) {
					outrow[i] = (double)(float)outrow[i];
				} else if (fields[i].getFieldType() == FieldType.INTEGER ) {
					outrow[i] = (long)(int)outrow[i];					
				}
			}
		}
		
		public boolean handleMessage(ByteBuffer message) {
			Object[] outRow = RowDataUtil.allocateRowData(m_data.m_outputRowMeta.size());
			
			if ( m_meta.getPassAsBinary() ) {
				// Get bytes directly into first output field
				// It had better be BINARY!!!
				outRow[0] = message.remaining();
			}
			else {
			    // Use the decoder
    			try {
				    m_data.m_decoder.DecodePacket(message, outRow);
				    reconfarbulate(outRow);
			    } catch (Exception ex) {
		    		logBasic("Caught exception in decoder: " + ex.toString());
	    			return false;
    			}
			}
			try {
				putRow(m_data.m_outputRowMeta, outRow); // putRow is synched according to javadoc
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
