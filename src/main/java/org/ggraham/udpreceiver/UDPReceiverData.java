package org.ggraham.udpreceiver;

import org.ggraham.message.PacketDecoder;
import org.ggraham.network.UDPReceiver;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.Date;

public class UDPReceiverData extends BaseStepData implements StepDataInterface {

  protected RowMetaInterface m_outputRowMeta;

  protected UDPReceiver m_receiver;
  protected PacketDecoder m_decoder;
  protected Object m_lock = new Object(); 
  
  protected long m_packetCount;
  protected long m_currentPacketCount;
  protected long m_executionDuration;
  protected Date m_startTime;
}
