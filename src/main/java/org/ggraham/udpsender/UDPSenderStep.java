package org.ggraham.udpsender;
/*

MIT License

Copyright (c) [2017] [Gregory Graham]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

import java.nio.ByteBuffer;
import java.util.HashMap;

import org.ggraham.message.PacketDecoder;
import org.ggraham.network.UDPSender;
import org.ggraham.objectpool.ObjectPool.PoolItem;
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
				configureConnection(uMeta, uData);
			}

			String[] fieldNames = uMeta.getFieldNames();
			Object[] toSend = new Object[fieldNames.length];
			for (int ii = 0; ii < fieldNames.length; ii++) {
				toSend[ii] = row[uData.m_fieldMap.get(ii)];
			}

			PoolItem<ByteBuffer> pBuffer = uData.m_sender.getByteBuffer();
			uData.m_decoder.EncodePacket(toSend, pBuffer.getPoolItem());
			uData.m_sender.send(pBuffer);
			logBasic("Sent Message");

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
			for (PacketDecoder.PacketFieldConfig f : meta.getFields()) {
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
