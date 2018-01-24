package org.ggraham.udpsender;
/*
 * 
 * Apache License 2.0 
 * 
 * Copyright (c) [2017] [Gregory Graham]
 * 
 * See LICENSE.txt for details.
 * 
 */

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.ggraham.ggutils.logging.DefaultLogger;
import org.ggraham.ggutils.logging.LogLevel;
import org.ggraham.ggutils.message.PacketDecoder;
import org.ggraham.ggutils.message.FieldType;
import org.ggraham.ggutils.message.PacketFieldConfig;
import org.ggraham.ggutils.network.UDPSender;
import org.ggraham.ggutils.objectpool.ObjectPool.PoolItem;
import org.ggraham.ggutils.PackageService;
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

/**
 * 
 * Implements StepInterface for UDPSender: sends UDP packets, encodes them from
 * rows containing data from the transformation.
 * 
 * @author ggraham
 *
 */
public class UDPSenderStep extends BaseStep implements StepInterface {

	public UDPSenderStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
			Trans trans) {
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	// Main row processor; checks for data and exits when there is no more data
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		UDPSenderData uData = (UDPSenderData) sdi;
		UDPSenderMeta uMeta = (UDPSenderMeta) smi;

		if (isStopped()) {
			return false;
		} else {

			// Get the row and check for null.
			// This has to be done before a call to getInputRowMeta()
			Object[] row = getRow();
			if (row == null) {
				setOutputDone();
				return false;
			}

			// Check for first time through
			if (first) {
				first = false;
				uData.m_inputRowMeta = getInputRowMeta();
				uData.m_fieldMap = new HashMap<Integer, Integer>();
				String[] fieldNames = uMeta.getFieldNames();
				for (int ii = 0; ii < fieldNames.length; ii++) {
					uData.m_fieldMap.put(ii, uData.m_inputRowMeta.indexOfValue(fieldNames[ii]));
				}
				final BaseStep bStep = this;
				PackageService.getPackageService().setLogImpl(new DefaultLogger() {
					@Override
					protected void doLog(String source, String message, int setLevel, int logLevel, String sLogLevel) {
					    bStep.logBasic(source + " " + message);
					}
				});
				PackageService.getLog().setLogLevel(LogLevel.BASIC);
				configureConnection(uMeta, uData);
			}

			String[] fieldNames = uMeta.getFieldNames();
			PacketFieldConfig[] fieldConfig = uMeta.getFields();
			Object[] toSend = new Object[fieldNames.length];
			for (int ii = 0; ii < fieldNames.length; ii++) {
				if ( fieldConfig[ii].getFieldType() == FieldType.INTEGER ) {
				    toSend[ii] = (int)(long)row[uData.m_fieldMap.get(ii)];
				} else	if ( fieldConfig[ii].getFieldType() == FieldType.FLOAT ) {
				    toSend[ii] = (float)(double)row[uData.m_fieldMap.get(ii)];					
				} else {
				    toSend[ii] = row[uData.m_fieldMap.get(ii)];										
				}
				
			}

			PoolItem<ByteBuffer> pBuffer = uData.m_sender.getByteBuffer();
			if ( uData.m_decoder.EncodePacket(toSend, pBuffer.getPoolItem()) ) {
    			uData.m_sender.send(pBuffer);
    			logDebug("Sent packet");
			}
			else {
				pBuffer.putBack();  // normally the sender does this
    			logError("Packet not sent because of encoding errors.");				
			}

			return true;
		}
	}

	protected void configureConnection(UDPSenderMeta meta, UDPSenderData data) throws KettleException {
		if (data.m_sender == null) {

			logBasic("Running configureConnection()...");
			String address = environmentSubstitute(meta.getAddress());
			if (Const.isEmpty(address)) {
				throw new KettleException(BaseMessages.getString(UDPSenderMeta.PKG, "UDPSenderStep.Error.NoIPAddress"));
			}

			String sPort = environmentSubstitute(meta.getPort());
			if (Const.isEmpty(sPort)) {
				throw new KettleException(BaseMessages.getString(UDPSenderMeta.PKG, "UDPSenderStep.Error.NoPort"));
			}
			int port = 0;
			try {
				port = Integer.parseInt(sPort);
			} catch (NumberFormatException ex) {
				throw new KettleException(
						BaseMessages.getString(UDPSenderMeta.PKG, "UDPSenderStep.Error.BadPort", sPort));
			}
			logBasic("IP Address is " + address + ":" + sPort);

			data.m_decoder = new PacketDecoder();
			for (PacketFieldConfig f : meta.getFields()) {
				data.m_decoder.addField(f);
			}

			String sInitPoolSize = environmentSubstitute(meta.getInitPoolSize());
			if (Const.isEmpty(sInitPoolSize)) {
				throw new KettleException(
						BaseMessages.getString(UDPSenderMeta.PKG, "UDPSenderStep.Error.NoInitPoolSize"));
			}
			int initPoolSize = 0;
			try {
				initPoolSize = Integer.parseInt(sInitPoolSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(UDPSenderMeta.PKG,
						"UDPSenderStep.Error.BadInitPoolSizeSize", sInitPoolSize));
			}
			logBasic("Initial Object Pool size is " + sInitPoolSize + " objects");

			String sMaxPoolSize = environmentSubstitute(meta.getMaxPoolSize());
			if (Const.isEmpty(sMaxPoolSize)) {
				throw new KettleException(
						BaseMessages.getString(UDPSenderMeta.PKG, "UDPSenderStep.Error.NoMaxPoolSize"));
			}
			int maxPoolSize = 0;
			try {
				maxPoolSize = Integer.parseInt(sMaxPoolSize);
			} catch (NumberFormatException ex) {
				throw new KettleException(BaseMessages.getString(UDPSenderMeta.PKG,
						"UDPSenderStep.Error.BadInitPoolSizeSize", sMaxPoolSize));
			}
			logBasic("Max Object Pool size is " + sMaxPoolSize + " objects");
			data.m_sender = new UDPSender(address, port, initPoolSize, maxPoolSize);
			logBasic("Created receiver " + (data.m_sender != null));

		}
	}

}
