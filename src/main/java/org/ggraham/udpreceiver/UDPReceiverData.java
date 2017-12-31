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
import org.ggraham.nsr.message.PacketDecoder;
import org.ggraham.nsr.network.UDPReceiver;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.util.Date;

/**
 * 
 * Implements the PDI StepDataInterface
 * 
 * @author ggraham
 *
 */
public class UDPReceiverData extends BaseStepData implements StepDataInterface {

  // For output row meta 
  protected RowMetaInterface m_outputRowMeta;

  // UDP packet receiver and decoder
  protected UDPReceiver m_receiver;
  protected PacketDecoder m_decoder;
  
  // Implements packet counter stop mechanism 
  protected Object m_lock = new Object();   
  protected long m_packetCount;
  protected long m_currentPacketCount;
  
  // Implements packet receiver livetime stop mechanism
  protected long m_executionDuration;
  protected Date m_startTime;
}
