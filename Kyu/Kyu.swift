//
//  Kyu.swift
//  Kyu
//
//  Created by Red Davis on 14/06/2015.
//  Copyright (c) 2015 Red Davis. All rights reserved.
//

import Foundation


/// Job arguments
public typealias KyuJobArguments = [String : AnyObject]

/**
    KyuJobResult
 
    - success: Job was successful, mark it as complete
    - fail:    Shit went bad
 */
public enum KyuJobResult
{
    case success, fail
}


/**
    KyuJobProtocol
 */
public protocol KyuJobProtocol
{
    func perform(_ arguments: KyuJobArguments) -> KyuJobResult
}


/**
    KyuConfigurationError
 */
public enum KyuConfigurationError: Error
{
    case jobNotProvided
    case directoryURLNotProvided
}


/**
    KyuJobManagementError
 */
public enum KyuJobManagementError: Error
{
    case jobNotFound
}


/**
    KyuConfiguration
 */
open class KyuConfiguration
{
    open var numberOfWorkers = 1
    open var job: KyuJobProtocol?
    open var directoryURL: URL?
    open var maximumNumberOfRetries = 0
}


/**
    Initialization errors
 
    - InvalidNumberOfWorkers: Invalid number of workers provided (e.g. 0)
 */
public enum KyuError: Error
{
    case invalidNumberOfWorkers
}


/**
    KyuDataSource
 */
public protocol KyuDataSource
{
    func kyuShouldIncrementRetryCount() -> Bool
}


/**
    KyuDelegate
 */
public protocol KyuDelegate
{
    func kyu(_ kyu: Kyu, didStartProcessingJob job: KyuJobViewModel)
    func kyu(_ kyu: Kyu, didFinishProcessingJob job: KyuJobViewModel, withResult result: KyuJobResult)
}


open class Kyu: KyuWorkerDataSource, KyuWorkerDelegate
{
    /// Pause/unpause processing jobs
    open var paused = false {
        didSet
        {
            for thread in self.workers
            {
                thread.paused = self.paused
            }
        }
    }
    
    /// Number of jobs queued
    open var numberOfJobs: Int {
        let total = self.workers.map { (worker) -> Int in
            return worker.numberOfJobs
        }.reduce(0) { (count, jobCount) -> Int in
            return count + jobCount
        }
        
        return total
    }
    
    /// Number of workers
    open var numberOfWorkers: Int {
        return self.workers.count
    }
    
    /// Data source
    open var dataSource: KyuDataSource?
    
    /// Delegate
    open var delegate: KyuDelegate?
    
    // Internal
    internal fileprivate(set) var workers = [KyuWorker]()
    
    // Private
    fileprivate let job: KyuJobProtocol
    fileprivate let baseDirectoryURL: URL
    fileprivate let maximumNumberOfRetries: Int
    
    // MARK: -
    
    /**
        Useful function for initializing a Kyu.
     
        - parameter configHandler: Use the KyuConfiguration object to configure the Kyu
     
        - throws: KyuConfigurationError, KyuWorkerInitializationError

        - returns: Configured Kyu object
     */
    open class func configure(_ configHandler: (_ config: KyuConfiguration) -> ()) throws -> Kyu
    {
        let configuration = KyuConfiguration()
        configHandler(configuration)
        
        guard let job = configuration.job else
        {
            throw KyuConfigurationError.jobNotProvided
        }
        
        guard let directoryURL = configuration.directoryURL else
        {
            throw KyuConfigurationError.directoryURLNotProvided
        }
        
        let numberOfWorkers = configuration.numberOfWorkers
        let maximumNumberOfRetries = configuration.maximumNumberOfRetries
        
        do
        {
            let kyu = try Kyu(numberOfWorkers: numberOfWorkers, job: job, directoryURL: directoryURL, maximumNumberOfRetries: maximumNumberOfRetries)
            return kyu
        }
        catch let error
        {
            throw error
        }
    }
    
    // MARK: Initialization
    
    /**
         Initialize a Kyu
         
         - parameter numberOfWorkers:        Number of workers
         - parameter job:                    Object that conforms to KyuJobProtocol protocol
         - parameter directoryURL:           NSURL to the Kyu directory
         - parameter maximumNumberOfRetries: Maxumum number of times to retry a job
         
         - throws: KyuConfigurationError, KyuWorkerInitializationError
         
         - returns: Kyu object
     */
    public required init(numberOfWorkers: Int, job: KyuJobProtocol, directoryURL: URL, maximumNumberOfRetries: Int) throws
    {
        self.job = job
        self.baseDirectoryURL = directoryURL
        self.maximumNumberOfRetries = maximumNumberOfRetries
        
        // Build workers
        var workers = [KyuWorker]()
        if numberOfWorkers > 0
        {
            for index in 1...numberOfWorkers
            {
                let worker = try KyuWorker(identifier: "\(index)", dataSource: self)
                worker.delegate = self
                workers.append(worker)
            }
        }
        
        self.workers = workers
        
        if self.workers.count < 1
        {
            throw KyuError.invalidNumberOfWorkers
        }
    }
    
    // MARK: Job management
    
    /**
         Queue a job
         
         - parameter arguments: Dictionary of arguments that you need for when the job is processed
         
         - returns: Job identifier, you can use this alongside the KyuDelegate to track when a job
                    has started/finished.
     */
    open func queueJob(_ arguments: KyuJobArguments) -> String
    {
        let worker = self.nextWorkerToAddJobTo()
        return worker.queueJob(arguments)
    }
    
    fileprivate func nextWorkerToAddJobTo() -> KyuWorker
    {
        let sortedWorkers = self.workers.sorted { (threadA: KyuWorker, threadB: KyuWorker) -> Bool in
            return threadA.numberOfJobs < threadB.numberOfJobs
        }
        
        return sortedWorkers.first!
    }
    
    /**
         Cancel a job. Note that jobs that have already started will not get cancelled.
         
         - parameter identifier: Job identifier
         
         - throws: KyuJobManagementError
     */
    open func cancelJob(_ identifier: String) throws
    {
        var results = [Bool]()
        for worker in self.workers
        {
            let result = worker.cancelJob(identifier)
            results.append(result)
        }
        
        if !results.contains(true)
        {
            throw KyuJobManagementError.jobNotFound
        }
    }
    
    // MARK: KyuWorkerDataSource
    
    internal func jobForKyuWorker(_ worker: KyuWorker) -> KyuJobProtocol
    {
        return self.job
    }
    
    internal func baseTemporaryDirectoryForKyuWorker(_ worker: KyuWorker) -> URL
    {
        let temporaryDirectory = URL(string: NSTemporaryDirectory())!
        return temporaryDirectory.appendingPathComponent("KyuTemp")
    }
    
    internal func baseJobDirectoryForKyuWorker(_ thread: KyuWorker) -> URL
    {
        return self.baseDirectoryURL
    }
    
    internal func maximumNumberOfRetriesForKyuWorker(_ worker: KyuWorker) -> Int
    {
        return self.maximumNumberOfRetries
    }
    
    internal func workerShouldIncrementRetryCounts() -> Bool
    {
        return self.dataSource?.kyuShouldIncrementRetryCount() ?? true
    }
    
    // MARK: KyuWorkerDelegate
    
    internal func worker(_ worker: KyuWorker, didStartProcessingJob job: KyuJob)
    {
        guard let unwrappedDelegate = self.delegate else { return }
        
        let jobViewModel = KyuJobViewModel(job: job)
        unwrappedDelegate.kyu(self, didStartProcessingJob: jobViewModel)
    }
    
    internal func worker(_ worker: KyuWorker, didFinishProcessingJob job: KyuJob, withResult result: KyuJobResult)
    {
        guard let unwrappedDelegate = self.delegate else { return }
        
        let jobViewModel = KyuJobViewModel(job: job)
        unwrappedDelegate.kyu(self, didFinishProcessingJob: jobViewModel, withResult: result)
    }
}
